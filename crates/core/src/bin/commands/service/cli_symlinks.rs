//! macOS: put `freenet`/`fdev` on `PATH` after a DMG install, automatically.
//!
//! The DMG only ever produces a drag-to-Applications `.app` bundle — there is
//! no `.pkg`/postinstall step (see `scripts/package-macos.sh`) to run
//! privileged setup at "install" time. The earliest hookable moment is first
//! *launch* of the wrapper, so that is where this runs. It is a ONE-SHOT,
//! best-effort action gated by its own marker
//! (`super::cli_symlinks_marker_path`), for the same reason the legacy-install
//! migration marker is separate from the onboarding first-run marker (see
//! `launch_at_login.rs`): a user who already onboarded via an older DMG still
//! needs this to run once, on their next launch after upgrading.
//!
//! Split into a pure decision core (testable on every CI platform) and a
//! macOS-only execution wrapper, following the `first_run_marker_*` /
//! `compute_menu_state` pattern documented in `.claude/rules/deployment.md`.

use std::path::Path;
#[cfg(target_os = "macos")]
use std::path::PathBuf;

/// Directory the symlinks are installed into. On the default `PATH` for
/// login shells on macOS (bash/zsh/sh via `/etc/paths`; fish's default
/// universal path list) with no shell-rc edit required — unlike
/// `~/.local/bin`, which `install.sh` has to warn about explicitly because it
/// is NOT on `PATH` by default.
pub(super) const CLI_BIN_DIR: &str = "/usr/local/bin";

/// One CLI tool this module keeps symlinked onto `PATH`: the symlink name
/// under [`CLI_BIN_DIR`] and the basename of the real binary inside the
/// running app bundle's `Contents/MacOS/` directory.
#[allow(dead_code)] // Only constructed by the macOS-only execution path below.
pub(super) struct ManagedTool {
    pub link_name: &'static str,
    pub bundle_binary_name: &'static str,
}

/// `freenet-bin` is the real multi-subcommand binary (`Contents/MacOS/Freenet`
/// is just a wrapper script that execs it with `service run-wrapper`), so
/// symlinking `freenet` straight to it gives a fully-functional CLI. `fdev`
/// is bundled as-is (see `package-macos.sh`); it may be ABSENT from an older
/// DMG built before fdev was bundled, which `bundle_binary_exists` below
/// handles as a no-op rather than an error.
#[allow(dead_code)] // Only read by the macOS-only execution path below.
pub(super) const MANAGED_TOOLS: &[ManagedTool] = &[
    ManagedTool {
        link_name: "freenet",
        bundle_binary_name: "freenet-bin",
    },
    ManagedTool {
        link_name: "fdev",
        bundle_binary_name: "fdev",
    },
];

/// Prompt text shown in the administrator-privileges dialog when unprivileged
/// symlink creation fails (i.e. `/usr/local/bin` isn't user-writable).
const CLI_SYMLINK_PROMPT: &str = "Freenet would like to install command-line tools (freenet, fdev) so you can use them from Terminal.";

// ── Pure decision core (unit-tested on every CI platform) ──────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum CliSymlinkAction {
    /// Nothing to do: either this tool isn't in the running bundle (e.g. an
    /// older DMG without `fdev`), or the link already points at the right
    /// place.
    NoOp,
    /// The link is missing, or is a stale symlink into a (possibly
    /// different-path, e.g. the user moved/renamed the app) Freenet bundle:
    /// safe to (re)create.
    CreateOrRepair,
    /// Something this installer does not own occupies the link path (a
    /// regular file, or a symlink to somewhere that isn't a Freenet app
    /// bundle). Leave it alone.
    SkipForeignOccupant,
}

/// Decide what to do about one managed CLI tool's symlink. Pure: all
/// filesystem state is passed in by the caller (via `std::fs::symlink_metadata`
/// / `std::fs::read_link`) so this is unit-testable on any platform without
/// touching the real filesystem.
///
/// - `bundle_binary_exists`: whether the real binary exists inside the
///   currently-running app bundle.
/// - `link_exists` / `link_is_symlink` / `link_target`: current state of the
///   symlink path.
/// - `expected_target`: the absolute path the symlink SHOULD point at,
///   derived from the currently-running executable's bundle directory.
#[allow(dead_code)]
pub(super) fn decide_cli_symlink_action(
    bundle_binary_exists: bool,
    link_exists: bool,
    link_is_symlink: bool,
    link_target: Option<&Path>,
    expected_target: &Path,
) -> CliSymlinkAction {
    if !bundle_binary_exists {
        return CliSymlinkAction::NoOp;
    }
    if !link_exists {
        return CliSymlinkAction::CreateOrRepair;
    }
    if !link_is_symlink {
        return CliSymlinkAction::SkipForeignOccupant;
    }
    // The basename check matters as much as the bundle-shape check below: a
    // path can be inside SOME `.app/Contents/MacOS/` without being a path we
    // recognize for THIS tool (e.g. an unrelated app that happens to ship a
    // binary named `freenet`). Requiring both means we only ever repoint a
    // link whose old target both looks like an app bundle binary AND was for
    // the same tool `expected_target` is for.
    match link_target {
        Some(t) if t == expected_target => CliSymlinkAction::NoOp,
        Some(t)
            if is_freenet_app_bundle_binary(t) && t.file_name() == expected_target.file_name() =>
        {
            CliSymlinkAction::CreateOrRepair
        }
        _ => CliSymlinkAction::SkipForeignOccupant,
    }
}

/// True if `path` looks like it points inside SOME `.app` bundle's
/// `Contents/MacOS/` directory — i.e. a symlink a past or current version of
/// this installer could plausibly have created, safe to repoint at the
/// currently-running bundle. Anything else (Homebrew, a hand-built binary, an
/// unrelated app) is left untouched: the same "never clobber an install we
/// don't own" reasoning as `is_legacy_freenet_wrapper`'s content-signature
/// check in `launch_at_login.rs`, just applied to a symlink target instead of
/// a script's contents. Callers MUST also compare basenames (see
/// `decide_cli_symlink_action`) — this check alone only rules out a symlink
/// that isn't inside any app bundle at all; it does not by itself prove the
/// bundle is Freenet's or that the binary is the same tool.
#[allow(dead_code)]
pub(super) fn is_freenet_app_bundle_binary(path: &Path) -> bool {
    path.to_string_lossy().contains(".app/Contents/MacOS/")
}

/// Build the AppleScript source for `osascript -e` that (a) ensures
/// [`CLI_BIN_DIR`] exists and (b) (re)points every `(target, link)` pair,
/// all under ONE administrator-privileges prompt. AppleScript's
/// `quoted form of` performs the shell quoting, so this Rust code never has
/// to hand-quote a path for a shell — each path is embedded exactly once, as
/// an AppleScript string literal via `applescript_escape`, matching the
/// existing `notify_stuck_wrapper` pattern in `wrapper.rs`. Pure string
/// building, unit-tested without ever invoking `osascript`.
#[allow(dead_code)]
pub(super) fn build_elevated_symlink_script(pairs: &[(&str, &str)]) -> String {
    let mut expr_parts = vec![format!(
        "\"mkdir -p \" & {}",
        applescript_quoted_form_of(CLI_BIN_DIR)
    )];
    for (target, link) in pairs {
        expr_parts.push(format!(
            "\" && ln -sf \" & {} & \" \" & {}",
            applescript_quoted_form_of(target),
            applescript_quoted_form_of(link),
        ));
    }
    let shell_expr = expr_parts.join(" & ");
    format!(
        "do shell script {shell_expr} with administrator privileges with prompt \"{prompt}\"",
        prompt = super::wrapper::applescript_escape(CLI_SYMLINK_PROMPT),
    )
}

/// `quoted form of "<escaped path>"` — an AppleScript expression fragment
/// that shell-quotes `path` at AppleScript-eval time. `path` is embedded as
/// an AppleScript string literal, so it goes through `applescript_escape`
/// first (backslash and double-quote), same as every other literal
/// interpolated into an `osascript -e` argument in this codebase.
#[allow(dead_code)]
fn applescript_quoted_form_of(path: &str) -> String {
    format!(
        "quoted form of \"{}\"",
        super::wrapper::applescript_escape(path)
    )
}

/// True if `path` is under Gatekeeper's App Translocation mount
/// (`/private/var/folders/.../AppTranslocation/<uuid>/d/...`). macOS
/// translocates a quarantined `.app` to a randomized, ephemeral path whenever
/// it is launched directly from its mounted disk image without first being
/// copied elsewhere (e.g. double-clicking `Freenet.app` inside the DMG's
/// Finder window instead of dragging it to `/Applications` first) — this is
/// one of the most common first-launch patterns for a DMG-distributed app.
/// `std::env::current_exe()` resolves to a path under the translocation
/// mount in that case, which is torn down once the disk image is ejected (or
/// replaced with a fresh random uuid on a later translocated launch). A
/// symlink created against that path would dangle, so the caller must skip
/// symlink setup — and critically must NOT write the one-shot marker — when
/// this returns true, or the fix would never get a chance to run again after
/// the user does the right thing (drags the app to `/Applications` and
/// relaunches from there).
#[allow(dead_code)]
pub(super) fn is_translocated_path(path: &Path) -> bool {
    path.to_string_lossy().contains("/AppTranslocation/")
}

// ── macOS-only execution ────────────────────────────────────────────────────

/// Desktop notification for the declined/failed elevated-install case,
/// same `osascript display notification` mechanism as `notify_stuck_wrapper`
/// in `wrapper.rs`. Best-effort: a notification failure must never block or
/// panic the wrapper.
#[cfg(target_os = "macos")]
fn notify_cli_symlink_declined() {
    let body = super::wrapper::applescript_escape(
        "freenet/fdev were not added to your PATH. See the Freenet log for the manual command to run.",
    );
    let title = super::wrapper::applescript_escape("Freenet command-line tools not installed");
    let script = format!("display notification \"{body}\" with title \"{title}\"");
    let _ = std::process::Command::new("osascript")
        .arg("-e")
        .arg(&script)
        // Null all three handles: the wrapper has detached from its console
        // (see the FreeConsole cross-reference in wrapper.rs::run_wrapper).
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// Best-effort, one-shot: ensure `freenet` and `fdev` are symlinked into
/// [`CLI_BIN_DIR`] so they're on `PATH` from Terminal. No-ops after the first
/// attempt (success, decline, or nothing-to-do) via
/// `super::cli_symlinks_marker_path`. Runs on its own background thread (see
/// call site in `wrapper.rs`) so a slow or declined administrator-privileges
/// prompt never delays the tray icon appearing.
#[cfg(target_os = "macos")]
pub(super) fn macos_ensure_cli_symlinks(log_dir: &Path) {
    use super::wrapper::log_wrapper_event;
    use super::{cli_symlinks_marker_path, mark_first_run_complete_at};

    let Some(marker) = cli_symlinks_marker_path() else {
        return;
    };
    if marker.exists() {
        return;
    }

    let Ok(exe) = std::env::current_exe() else {
        log_wrapper_event(
            log_dir,
            "CLI symlink setup skipped: could not resolve current executable path",
        );
        return;
    };
    if is_translocated_path(&exe) {
        // Do NOT write the marker: this is not a permanent state. The next
        // launch (e.g. after the user drags the app to /Applications, or
        // even a later re-translocated launch) gets another chance.
        log_wrapper_event(
            log_dir,
            "CLI symlink setup skipped: app is running from Gatekeeper's App \
             Translocation mount (launched directly from the DMG rather than \
             /Applications). Will retry on a future non-translocated launch.",
        );
        return;
    }
    let Some(macos_dir) = exe.parent() else {
        return;
    };

    let mut needs_repair: Vec<(PathBuf, PathBuf)> = Vec::new(); // (target, link)
    let mut skipped_foreign: Vec<PathBuf> = Vec::new();

    for tool in MANAGED_TOOLS {
        let target = macos_dir.join(tool.bundle_binary_name);
        let bundle_binary_exists = target.exists();
        let link = PathBuf::from(CLI_BIN_DIR).join(tool.link_name);
        let (link_exists, link_is_symlink, link_target) = match std::fs::symlink_metadata(&link) {
            Ok(meta) => {
                let is_symlink = meta.file_type().is_symlink();
                let target = if is_symlink {
                    std::fs::read_link(&link).ok()
                } else {
                    None
                };
                (true, is_symlink, target)
            }
            Err(_) => (false, false, None),
        };
        match decide_cli_symlink_action(
            bundle_binary_exists,
            link_exists,
            link_is_symlink,
            link_target.as_deref(),
            &target,
        ) {
            CliSymlinkAction::NoOp => {}
            CliSymlinkAction::CreateOrRepair => needs_repair.push((target, link)),
            CliSymlinkAction::SkipForeignOccupant => skipped_foreign.push(link),
        }
    }

    for link in &skipped_foreign {
        log_wrapper_event(
            log_dir,
            &format!(
                "CLI symlink setup: {} already exists and isn't managed by Freenet; leaving it \
                 alone. Run manually if you want the Freenet CLI on PATH there.",
                link.display()
            ),
        );
    }

    if needs_repair.is_empty() {
        if let Err(e) = mark_first_run_complete_at(&marker) {
            log_wrapper_event(log_dir, &format!("Failed to write CLI symlink marker: {e}"));
        }
        return;
    }

    // Attempt 1: unprivileged. Works when CLI_BIN_DIR is already
    // user-writable (e.g. Homebrew created it earlier, or a stale symlink we
    // own is being repointed and its containing directory is writable).
    let mut still_needed: Vec<(PathBuf, PathBuf)> = Vec::new();
    if std::fs::create_dir_all(CLI_BIN_DIR).is_ok() {
        for (target, link) in &needs_repair {
            let _ = std::fs::remove_file(link); // fine if absent
            match std::os::unix::fs::symlink(target, link) {
                Ok(()) => log_wrapper_event(
                    log_dir,
                    &format!(
                        "CLI symlink setup: linked {} -> {}",
                        link.display(),
                        target.display()
                    ),
                ),
                Err(_) => still_needed.push((target.clone(), link.clone())),
            }
        }
    } else {
        still_needed = needs_repair;
    }

    // Attempt 2: elevated, ONE administrator-privileges prompt covering
    // everything attempt 1 couldn't do unprivileged. Tracks whether osascript
    // itself failed to even RUN (as opposed to running and the user
    // declining, or the shell command failing) — that distinction decides
    // whether the one-shot marker below gets written: a transient inability
    // to spawn osascript (e.g. momentary resource exhaustion) must not
    // permanently disable this feature the way a deliberate user decline
    // reasonably should.
    let mut osascript_spawn_failed = false;
    if !still_needed.is_empty() {
        let pairs: Vec<(&str, &str)> = still_needed
            .iter()
            .map(|(t, l)| {
                (
                    t.to_str().unwrap_or_default(),
                    l.to_str().unwrap_or_default(),
                )
            })
            .collect();
        let script = build_elevated_symlink_script(&pairs);
        // Null all three handles: the wrapper has detached from its console
        // (see the FreeConsole cross-reference in wrapper.rs::run_wrapper).
        match std::process::Command::new("osascript")
            .arg("-e")
            .arg(&script)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
        {
            Ok(status) if status.success() => log_wrapper_event(
                log_dir,
                "CLI symlink setup: installed via administrator privileges",
            ),
            Ok(_) => {
                log_wrapper_event(
                    log_dir,
                    &format!(
                        "CLI symlink setup: declined or failed under administrator privileges. \
                         Run manually, e.g.: sudo ln -sf <bundle-binary> <link> for: {}",
                        still_needed
                            .iter()
                            .map(|(_, l)| l.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                );
                // The log line above is invisible to a GUI menu-bar app user
                // with no terminal open, and the one-shot marker means this
                // is the only signal they get that freenet/fdev did NOT end
                // up on PATH — so also surface it as a desktop notification.
                notify_cli_symlink_declined();
            }
            Err(e) => {
                osascript_spawn_failed = true;
                log_wrapper_event(
                    log_dir,
                    &format!(
                        "CLI symlink setup: failed to run osascript ({e}); will retry on next launch."
                    ),
                );
            }
        }
    }

    if osascript_spawn_failed {
        return;
    }

    // One-shot for everything else — never auto-prompt again. A user who
    // declines can still run the manual command logged above; a later launch
    // will not nag them a second time.
    if let Err(e) = mark_first_run_complete_at(&marker) {
        log_wrapper_event(log_dir, &format!("Failed to write CLI symlink marker: {e}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_link_is_create_or_repair() {
        let target = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            decide_cli_symlink_action(true, false, false, None, target),
            CliSymlinkAction::CreateOrRepair
        );
    }

    #[test]
    fn bundle_missing_binary_is_noop_even_if_link_missing() {
        // e.g. `fdev` in an older DMG that never bundled it.
        let target = Path::new("/Applications/Freenet.app/Contents/MacOS/fdev");
        assert_eq!(
            decide_cli_symlink_action(false, false, false, None, target),
            CliSymlinkAction::NoOp
        );
    }

    #[test]
    fn already_correct_link_is_noop() {
        let target = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            decide_cli_symlink_action(true, true, true, Some(target), target),
            CliSymlinkAction::NoOp
        );
    }

    #[test]
    fn stale_link_into_moved_app_is_repaired() {
        // App moved from /Applications to ~/Applications (or was renamed);
        // the existing symlink still points at the OLD bundle location.
        let old_target =
            Path::new("/Users/alice/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        let new_target = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            decide_cli_symlink_action(true, true, true, Some(old_target), new_target),
            CliSymlinkAction::CreateOrRepair
        );
    }

    #[test]
    fn foreign_regular_file_is_skipped() {
        let target = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            decide_cli_symlink_action(true, true, false, None, target),
            CliSymlinkAction::SkipForeignOccupant
        );
    }

    #[test]
    fn foreign_symlink_elsewhere_is_skipped() {
        // e.g. a Homebrew-installed unrelated `freenet` binary.
        let target = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        let unrelated = Path::new("/opt/homebrew/Cellar/some-other-freenet/1.0/bin/freenet");
        assert_eq!(
            decide_cli_symlink_action(true, true, true, Some(unrelated), target),
            CliSymlinkAction::SkipForeignOccupant
        );
    }

    #[test]
    fn broken_symlink_into_bundle_is_repaired() {
        // read_link succeeds even when the target no longer exists.
        let old_target = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        let new_target = Path::new("/Applications/Freenet 2.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            decide_cli_symlink_action(true, true, true, Some(old_target), new_target),
            CliSymlinkAction::CreateOrRepair
        );
    }

    #[test]
    fn app_bundle_binary_detection() {
        assert!(is_freenet_app_bundle_binary(Path::new(
            "/Applications/Freenet.app/Contents/MacOS/freenet-bin"
        )));
        assert!(is_freenet_app_bundle_binary(Path::new(
            "/Users/alice/Applications/Freenet 2.app/Contents/MacOS/fdev"
        )));
        assert!(!is_freenet_app_bundle_binary(Path::new(
            "/opt/homebrew/Cellar/freenet/1.0/bin/freenet"
        )));
        assert!(!is_freenet_app_bundle_binary(Path::new(
            "/Users/alice/.local/bin/freenet"
        )));
    }

    #[test]
    fn elevated_script_quotes_every_path_via_quoted_form() {
        let script = build_elevated_symlink_script(&[(
            "/Applications/Freenet.app/Contents/MacOS/freenet-bin",
            "/usr/local/bin/freenet",
        )]);
        assert!(script.starts_with("do shell script "));
        assert!(script.contains("with administrator privileges"));
        assert!(script.contains("mkdir -p"));
        assert!(
            script.contains(
                "quoted form of \"/Applications/Freenet.app/Contents/MacOS/freenet-bin\""
            )
        );
        assert!(script.contains("quoted form of \"/usr/local/bin/freenet\""));
        // The shell command is built entirely from `&`-concatenated
        // AppleScript expressions (literals + `quoted form of`), never a
        // path spliced directly into a shell string.
        assert!(!script.contains("ln -sf /Applications"));
    }

    #[test]
    fn elevated_script_covers_multiple_pairs_under_one_prompt() {
        let script = build_elevated_symlink_script(&[
            (
                "/Applications/Freenet.app/Contents/MacOS/freenet-bin",
                "/usr/local/bin/freenet",
            ),
            (
                "/Applications/Freenet.app/Contents/MacOS/fdev",
                "/usr/local/bin/fdev",
            ),
        ]);
        // Exactly one `with administrator privileges` — a single prompt for
        // both symlinks, not one per tool.
        assert_eq!(script.matches("with administrator privileges").count(), 1);
        assert_eq!(script.matches("ln -sf").count(), 2);
        assert!(script.contains("quoted form of \"/usr/local/bin/fdev\""));
    }

    #[test]
    fn elevated_script_escapes_quotes_in_paths() {
        // Pathological but possible: a renamed .app containing a quote.
        let script = build_elevated_symlink_script(&[(
            "/Applications/Freenet \"weird\".app/Contents/MacOS/freenet-bin",
            "/usr/local/bin/freenet",
        )]);
        assert!(script.contains("Freenet \\\"weird\\\".app"));
    }

    #[test]
    fn elevated_script_escapes_backslashes_in_paths() {
        // A literal backslash is legal in an APFS/HFS+ filename. Escaping
        // ORDER matters here (backslash must be escaped before quotes, or a
        // quote's escaping backslash gets double-escaped) — this exercises
        // that independently of the quote-escaping test above, since the
        // pathological-quote fixture contains no literal backslash and so
        // cannot catch an order regression on its own.
        let script = build_elevated_symlink_script(&[(
            "/Applications/Weird\\Folder.app/Contents/MacOS/freenet-bin",
            "/usr/local/bin/freenet",
        )]);
        assert!(script.contains("Weird\\\\Folder.app"));
    }

    #[test]
    fn elevated_script_escapes_backslash_and_quote_together_in_correct_order() {
        // A single fixture containing BOTH special characters together, so a
        // regression that swaps applescript_escape's replace order (quote
        // before backslash, instead of backslash before quote) is actually
        // detectable: escaping backslash-then-quote turns `\"` into 3
        // backslashes + a quote; escaping quote-then-backslash instead
        // produces 4 backslashes + a quote. The two split tests above each
        // contain only ONE of the two characters, so neither can tell these
        // orders apart on its own — this is the test that actually pins it.
        let script = build_elevated_symlink_script(&[(
            "Weird\\\"Folder.app/Contents/MacOS/freenet-bin",
            "/usr/local/bin/freenet",
        )]);
        assert!(script.contains(r#"Weird\\\"Folder.app"#));
        assert!(!script.contains(r#"Weird\\\\\"Folder.app"#));
    }

    #[test]
    fn different_tool_in_same_bundle_shape_is_skipped() {
        // The existing link's target is inside SOME .app/Contents/MacOS/ (so
        // `is_freenet_app_bundle_binary` alone would say yes) but for a
        // DIFFERENT tool than what `freenet` should point at — must not be
        // silently repointed to the wrong binary.
        let existing = Path::new("/Applications/Freenet.app/Contents/MacOS/fdev");
        let expected = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            decide_cli_symlink_action(true, true, true, Some(existing), expected),
            CliSymlinkAction::SkipForeignOccupant
        );
    }

    #[test]
    fn same_tool_different_app_bundle_is_still_repaired_by_design() {
        // Accepted residual risk, NOT a bug: if a DIFFERENT app's bundle
        // happens to ship a binary with the exact same basename inside its
        // own Contents/MacOS/ (e.g. "freenet-bin"), the basename+bundle-shape
        // check alone can't distinguish it from an old link into a prior
        // Freenet.app location, so this is still classified CreateOrRepair.
        // Judged acceptable because the symlink TARGET is always our own
        // trusted binary — repointing here only reclaims a path claim, it
        // never substitutes code. Full hardening would check bundle identity
        // (e.g. Info.plist CFBundleIdentifier); left as a follow-up given how
        // unlikely a same-named-binary collision across unrelated apps is.
        // Pinned explicitly so a future editor sees this is deliberate, not
        // overlooked.
        let existing = Path::new("/Applications/SomeOtherApp.app/Contents/MacOS/freenet-bin");
        let expected = Path::new("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            decide_cli_symlink_action(true, true, true, Some(existing), expected),
            CliSymlinkAction::CreateOrRepair
        );
    }

    #[test]
    fn translocated_path_is_detected() {
        assert!(is_translocated_path(Path::new(
            "/private/var/folders/zz/abc123/T/AppTranslocation/1234-5678/d/Freenet.app/Contents/MacOS/freenet-bin"
        )));
        assert!(!is_translocated_path(Path::new(
            "/Applications/Freenet.app/Contents/MacOS/freenet-bin"
        )));
    }
}

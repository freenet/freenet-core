use std::path::{Path, PathBuf};

// log_wrapper_event lives in the sibling `wrapper` module and is needed
// by the macOS-only launch-at-login side-effecting helpers here.
#[cfg(target_os = "macos")]
use super::wrapper::log_wrapper_event;

// First-run / legacy-migration marker helpers live in the parent `service`
// module (platform-independent so they can be unit-tested on Linux CI); the
// macOS startup self-heal path below references them.
#[cfg(target_os = "macos")]
use super::{
    first_run_marker_path, is_first_run_at, legacy_migration_marker_path,
    mark_first_run_complete_at,
};

// ── Launch at Login (macOS) ──
//
// On macOS, the DMG-installed Freenet.app does not auto-start on login by
// itself: the .app bundle is just a folder in /Applications. To match
// the Windows-installer experience (service registers with the OS and
// starts on boot) we write a user LaunchAgent plist that points at the
// .app bundle's CFBundleExecutable shell wrapper and sets RunAtLoad=true.
//
// The presence/absence of the plist file is the single source of truth
// for the user's preference: plist exists means launch at login enabled.
// The tray menu's Launch at Login check item reads this state.
//
// Implementation uses `launchctl bootstrap gui/$UID <plist>`, which loads
// the agent into the current GUI session and honours RunAtLoad on future
// logins. launchctl exit status is logged via tracing::warn for diagnosis
// but not propagated to callers; the plist is the authoritative preference.

/// Identifier that also serves as the plist filename and launchd label.
#[allow(dead_code)]
const LAUNCH_AT_LOGIN_LABEL: &str = "org.freenet.Freenet";

/// Legacy plist label written by the old `install.sh` / `freenet service
/// install` path. We log a warning when we detect it so users know to
/// clean up the duplicate before it races for port 7509.
#[allow(dead_code)]
const LEGACY_SERVICE_LAUNCHD_LABEL: &str = "org.freenet.node";

/// Absolute path of the user LaunchAgent plist, if `$HOME` is resolvable.
#[allow(dead_code)]
fn launch_agent_plist_path() -> Option<PathBuf> {
    launch_agent_plist_path_for(LAUNCH_AT_LOGIN_LABEL)
}

#[allow(dead_code)]
fn launch_agent_plist_path_for(label: &str) -> Option<PathBuf> {
    dirs::home_dir().map(|h| {
        h.join("Library")
            .join("LaunchAgents")
            .join(format!("{label}.plist"))
    })
}

#[allow(dead_code)]
pub(super) fn is_launch_at_login_enabled_at(plist: &Path) -> bool {
    plist.exists()
}

/// If `exe` lives inside a macOS `.app` bundle, return that bundle's
/// absolute path. Walks up the parent directories until a path segment
/// ending in `.app` is found, or the filesystem root is reached.
///
/// Exposed at `pub(super)` so `update.rs` can detect bundle context for
/// its DMG-swap auto-update path.
#[allow(dead_code)]
pub(crate) fn macos_app_bundle_path(exe: &Path) -> Option<PathBuf> {
    for ancestor in exe.ancestors() {
        if ancestor
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(".app"))
        {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

/// The path to a `.app` bundle's CFBundleExecutable shell wrapper, by our
/// packaging convention. See `scripts/package-macos.sh` for how this
/// wrapper is produced: it execs the inner `freenet-bin` with the
/// `service run-wrapper` subcommand.
#[allow(dead_code)]
fn macos_app_bundle_wrapper(bundle: &Path) -> PathBuf {
    bundle.join("Contents").join("MacOS").join("Freenet")
}

/// Compute the ProgramArguments array for the user LaunchAgent. If we're
/// running from inside an `.app` bundle, launchd should relaunch the
/// bundle's CFBundleExecutable shell wrapper (which in turn execs
/// `freenet-bin service run-wrapper`, entering the tray path). Otherwise
/// (cargo-run, raw binary install, dev build) launchd needs to invoke
/// the binary with the explicit `service run-wrapper` subcommand or it
/// would default to `Network` mode and skip the tray entirely.
#[allow(dead_code)]
pub(super) fn launch_agent_program_arguments(exe: &Path) -> Vec<String> {
    match macos_app_bundle_path(exe) {
        Some(bundle) => vec![
            macos_app_bundle_wrapper(&bundle)
                .to_string_lossy()
                .into_owned(),
        ],
        None => vec![
            exe.to_string_lossy().into_owned(),
            "service".to_string(),
            "run-wrapper".to_string(),
        ],
    }
}

fn xml_escape(s: &str) -> String {
    // XML 1.0 predefined entities. Filesystem paths very rarely contain
    // these, but launchd silently fails to load a malformed plist, so
    // defensive escaping avoids a hard-to-diagnose "Launch at Login
    // silently does nothing" for pathological paths.
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// Build the plist XML for the user LaunchAgent given explicit
/// ProgramArguments. Pure function so the output format is unit-testable
/// without touching the filesystem or launchctl.
#[allow(dead_code)]
pub(super) fn launch_agent_plist_contents(program_arguments: &[String]) -> String {
    let mut args_xml = String::new();
    for arg in program_arguments {
        args_xml.push_str("        <string>");
        args_xml.push_str(&xml_escape(arg));
        args_xml.push_str("</string>\n");
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LAUNCH_AT_LOGIN_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
{args_xml}    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>ProcessType</key>
    <string>Interactive</string>
</dict>
</plist>
"#
    )
}

/// Write the plist to disk, creating `~/Library/LaunchAgents` if needed.
/// Does NOT activate the agent with launchctl: that's the caller's job
/// (separated so the filesystem half is unit-testable on Linux).
#[allow(dead_code)]
pub(super) fn write_launch_agent_plist_at(
    plist: &Path,
    program_arguments: &[String],
) -> std::io::Result<()> {
    if let Some(parent) = plist.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(plist, launch_agent_plist_contents(program_arguments))
}

/// Remove the plist. Idempotent: returns Ok if the file was already gone.
#[allow(dead_code)]
pub(super) fn remove_launch_agent_plist_at(plist: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(plist) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Returns true if the on-disk plist already contains the expected
/// leading ProgramArguments path, false otherwise (including on read
/// error). Used for the self-heal check at wrapper startup so the
/// plist gets rewritten after the user moves or replaces the .app.
#[allow(dead_code)]
pub(super) fn launch_agent_plist_has_expected_leader(plist: &Path, expected_leader: &str) -> bool {
    let escaped = format!("<string>{}</string>", xml_escape(expected_leader));
    match std::fs::read_to_string(plist) {
        Ok(contents) => contents.contains(&escaped),
        Err(_) => false,
    }
}

/// Call `launchctl bootstrap gui/$UID <plist>`. Logs non-zero exit
/// (with stderr) via tracing::warn so silent "plist written but launchd
/// never loaded it" breakages are diagnosable after the fact.
#[cfg(target_os = "macos")]
fn launchctl_bootstrap(plist: &Path) {
    // SAFETY: libc::getuid is always safe; it returns the current user
    // ID without touching user-provided pointers.
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    match std::process::Command::new("launchctl")
        .args(["bootstrap", &target])
        .arg(plist)
        .output()
    {
        Ok(o) if !o.status.success() => {
            tracing::warn!(
                "launchctl bootstrap {} returned {}: {}",
                plist.display(),
                o.status,
                String::from_utf8_lossy(&o.stderr).trim()
            );
        }
        Err(e) => {
            tracing::warn!(
                "launchctl bootstrap {} failed to spawn: {}",
                plist.display(),
                e
            );
        }
        _ => {}
    }
}

#[cfg(target_os = "macos")]
fn launchctl_bootout(plist: &Path) {
    // SAFETY: libc::getuid is always safe; it returns the current user
    // ID without touching user-provided pointers.
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    match std::process::Command::new("launchctl")
        .args(["bootout", &target])
        .arg(plist)
        .output()
    {
        Ok(o) if !o.status.success() => {
            // bootout returns non-zero when the agent isn't currently
            // loaded, which is expected when we're disabling after a
            // previous session already exited. Log at debug, not warn.
            tracing::debug!(
                "launchctl bootout {} returned {}: {}",
                plist.display(),
                o.status,
                String::from_utf8_lossy(&o.stderr).trim()
            );
        }
        Err(e) => {
            tracing::warn!(
                "launchctl bootout {} failed to spawn: {}",
                plist.display(),
                e
            );
        }
        _ => {}
    }
}

#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
fn launchctl_bootstrap(_plist: &Path) {}

#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
fn launchctl_bootout(_plist: &Path) {}

/// Enable launch-at-login: write the plist and ask launchd to load it.
#[allow(dead_code)]
pub(crate) fn enable_launch_at_login(executable: &Path) -> std::io::Result<()> {
    let Some(plist) = launch_agent_plist_path() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "could not resolve home directory for launch agent path",
        ));
    };
    let args = launch_agent_program_arguments(executable);
    // If an older plist is already loaded, bootout first so bootstrap
    // picks up any change to the executable path (e.g. user moved the
    // .app from /Applications to ~/Applications between launches).
    if plist.exists() {
        launchctl_bootout(&plist);
    }
    write_launch_agent_plist_at(&plist, &args)?;
    launchctl_bootstrap(&plist);
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn disable_launch_at_login() -> std::io::Result<()> {
    let Some(plist) = launch_agent_plist_path() else {
        return Ok(());
    };
    launchctl_bootout(&plist);
    remove_launch_agent_plist_at(&plist)
}

/// User-facing query: is launch-at-login currently on?
#[allow(dead_code)]
pub(crate) fn is_launch_at_login_enabled() -> bool {
    launch_agent_plist_path()
        .map(|p| is_launch_at_login_enabled_at(&p))
        .unwrap_or(false)
}

/// Decision outcome for first-run Launch at Login registration. Pure so
/// the decision logic can be unit-tested independently of filesystem or
/// launchctl side-effects, per the deployment-rule pattern established
/// earlier in this PR.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum FirstRunLaunchAtLoginAction {
    /// User hasn't launched before and Launch at Login is disabled: enable it.
    Register,
    /// User hasn't launched before but Launch at Login is already enabled
    /// (e.g. from the legacy install.sh path). Leave as-is.
    AlreadyEnabled,
    /// Not a first-run invocation.
    NotFirstRun,
}

#[allow(dead_code)]
pub(super) fn first_run_launch_at_login_action(
    is_first_run: bool,
    already_enabled: bool,
) -> FirstRunLaunchAtLoginAction {
    if !is_first_run {
        FirstRunLaunchAtLoginAction::NotFirstRun
    } else if already_enabled {
        FirstRunLaunchAtLoginAction::AlreadyEnabled
    } else {
        FirstRunLaunchAtLoginAction::Register
    }
}

/// Decision outcome for a user-initiated Launch at Login toggle (click
/// on the tray menu check item). Pure function, same testability
/// rationale as `first_run_launch_at_login_action`.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ToggleLaunchAtLoginOutcome {
    Enable,
    Disable,
}

#[allow(dead_code)]
pub(crate) fn toggle_launch_at_login_outcome(
    currently_enabled: bool,
) -> ToggleLaunchAtLoginOutcome {
    if currently_enabled {
        ToggleLaunchAtLoginOutcome::Disable
    } else {
        ToggleLaunchAtLoginOutcome::Enable
    }
}

/// Startup self-heal: if Launch at Login is enabled but the plist points
/// somewhere other than where we'd write it now (user moved the .app,
/// upgraded via a new DMG install location, etc.), rewrite it. Idempotent:
/// no-op when the plist is already current, or when Launch at Login is
/// disabled, or when we can't figure out the expected location.
#[allow(dead_code)]
pub(super) fn refresh_launch_at_login_plist_if_stale() {
    if !is_launch_at_login_enabled() {
        return;
    }
    let Some(plist) = launch_agent_plist_path() else {
        return;
    };
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let args = launch_agent_program_arguments(&exe);
    let Some(leader) = args.first() else {
        return;
    };
    if launch_agent_plist_has_expected_leader(&plist, leader) {
        return;
    }
    tracing::info!(
        "Refreshing stale Launch at Login plist (was pointing elsewhere, now {})",
        leader
    );
    if let Err(e) = enable_launch_at_login(&exe) {
        tracing::warn!("Failed to refresh Launch at Login plist: {}", e);
    }
}

/// Returns true if the legacy `org.freenet.node` LaunchAgent plist
/// exists. Used to warn users migrating from `install.sh` that they
/// have two auto-starts racing for the same ports.
#[allow(dead_code)]
pub(super) fn legacy_launchd_agent_present() -> bool {
    launch_agent_plist_path_for(LEGACY_SERVICE_LAUNCHD_LABEL)
        .map(|p| p.exists())
        .unwrap_or(false)
}

// ── Legacy install.sh → DMG migration (issue #3943) ──
//
// Users who installed Freenet via the old `install.sh` path on macOS have a
// legacy launchd agent (`~/Library/LaunchAgents/org.freenet.node.plist`) that
// auto-starts a legacy CLI binary on login, plus that binary somewhere on
// disk (`~/.local/bin/freenet` by default, `/usr/local/bin/freenet`, etc.).
//
// When such a user installs the signed DMG, both auto-starts race for port
// 7509 on every login: launchd respawns the legacy backend, the DMG wrapper's
// `kill_stale_freenet_processes` pkills it, launchd respawns it again, and the
// new wrapper's backend loses the port race and exits 43.
//
// On first DMG launch we migrate by removing the legacy plist (after booting
// out its launchd agent) and the legacy CLI binaries, then letting the normal
// first-run flow register the new `org.freenet.Freenet` agent.
//
// The DECISION (what to remove) is a pure function so it is unit-testable on
// Linux CI with a mock `$HOME`; the EXECUTION (launchctl bootout, fs::remove)
// is the macOS-only side-effecting wrapper. This split follows the
// deployment-rule pattern (`first_run_marker_*`, `compute_menu_state`).

/// A side effect the legacy-install migration should perform, in the order
/// listed by [`legacy_install_migration_plan`]. Each variant names exactly
/// one filesystem object so the executor can attempt them independently and
/// fall back to a manual-cleanup warning per object (e.g. a root-owned
/// binary the user can't `rm` without `sudo`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum LegacyMigrationStep {
    /// Boot out the legacy launchd agent for this plist, then remove the
    /// plist file. Bootout must precede removal so launchd stops respawning
    /// the legacy backend before we delete its definition.
    BootOutAndRemovePlist(PathBuf),
    /// Remove the legacy auto-update wrapper script the plist invoked
    /// (`freenet-service-wrapper.sh`).
    RemoveLegacyWrapperScript(PathBuf),
    /// Remove a legacy CLI binary that THIS legacy install owned (derived
    /// from the legacy install's own artifacts, never a guessed location —
    /// see [`legacy_install_migration_plan`]).
    RemoveLegacyBinary(PathBuf),
}

/// What to do about a detected legacy install. Pure value computed by
/// [`legacy_install_migration_plan`]; the executor turns it into side
/// effects. `NoMigration` is its own variant (rather than an empty step
/// list) so the caller can log "nothing to migrate" distinctly from a
/// plan that ended up empty by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum LegacyMigrationPlan {
    /// No legacy plist present, or not a first-run invocation: do nothing.
    NoMigration,
    /// A legacy install was detected; perform these steps in order.
    Migrate(Vec<LegacyMigrationStep>),
}

/// The legacy install's own artifacts, resolved from its plist and wrapper
/// script (NOT guessed from hard-coded locations). The caller resolves these
/// by reading the legacy plist's `ProgramArguments` and the wrapper script it
/// points at, so the plan only ever touches files THIS legacy install owned.
///
/// This is the input that lets [`legacy_install_migration_plan`] stay pure:
/// the caller does the file I/O and existence checks, the planner just decides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct ResolvedLegacyInstall {
    /// The wrapper script path from the plist's `ProgramArguments`, if it was
    /// parseable AND the file exists. Removed by the migration.
    pub wrapper_script: Option<PathBuf>,
    /// Legacy CLI binaries that exist AND were referenced by the legacy
    /// install (the binary embedded in the wrapper script, plus its sibling
    /// `fdev`). Empty when the wrapper couldn't be read/parsed.
    pub binaries: Vec<PathBuf>,
}

/// Decide how to migrate a pre-existing `install.sh` legacy install. PURE:
/// takes the relevant filesystem state as inputs and returns a plan, with no
/// side effects, so it can be unit-tested on any platform with a mock `$HOME`.
///
/// Arguments:
/// - `migration_already_done`: whether the dedicated legacy-migration marker
///   is present (we already migrated on a previous launch). This is NOT the
///   onboarding first-run marker: a user who already launched a DMG before
///   this migration shipped has the first-run marker set but may still have
///   the racing legacy agent, so gating on first-run would skip exactly the
///   users who need the migration (Codex P1 on PR #4448). The migration is
///   its own one-shot tracked by its own marker.
/// - `legacy_plist`: the legacy plist path and whether it currently exists.
///   `None` means `$HOME` was unresolvable (no migration possible).
/// - `resolved`: the legacy install's OWN artifacts (wrapper script + the
///   binaries it referenced), already resolved and existence-filtered by the
///   caller. We deliberately do NOT remove binaries from guessed locations:
///   a user may have an unrelated `freenet`/`fdev` (Homebrew, cargo, manual)
///   in `/usr/local/bin` or `~/bin` while the legacy service used
///   `~/.local/bin`, and deleting that unrelated binary would be a
///   destructive side effect beyond fixing the launchd race (Codex P2 on
///   PR #4448). Targeting only the legacy install's own artifacts avoids that.
///
/// Returns `NoMigration` unless the migration hasn't been done yet AND the
/// legacy plist exists. We anchor migration on the plist (not on a stray
/// binary) because the plist is the thing that actively races for the port on
/// login; a lone legacy binary with no auto-start agent is harmless and left
/// alone.
#[allow(dead_code)]
pub(super) fn legacy_install_migration_plan(
    migration_already_done: bool,
    legacy_plist: Option<(PathBuf, bool)>,
    resolved: &ResolvedLegacyInstall,
) -> LegacyMigrationPlan {
    if migration_already_done {
        return LegacyMigrationPlan::NoMigration;
    }
    let Some((plist_path, plist_exists)) = legacy_plist else {
        return LegacyMigrationPlan::NoMigration;
    };
    if !plist_exists {
        return LegacyMigrationPlan::NoMigration;
    }

    let mut steps = Vec::with_capacity(2 + resolved.binaries.len());
    // Bootout + plist removal first: stop launchd respawning the legacy
    // backend before anything else.
    steps.push(LegacyMigrationStep::BootOutAndRemovePlist(plist_path));
    if let Some(wrapper) = &resolved.wrapper_script {
        steps.push(LegacyMigrationStep::RemoveLegacyWrapperScript(
            wrapper.clone(),
        ));
    }
    for bin in &resolved.binaries {
        steps.push(LegacyMigrationStep::RemoveLegacyBinary(bin.clone()));
    }
    LegacyMigrationPlan::Migrate(steps)
}

/// Extract the first `ProgramArguments` entry from a launchd plist's XML.
/// For the legacy `org.freenet.node` plist this is the auto-update wrapper
/// script path (`~/.local/bin/freenet-service-wrapper.sh`); see
/// [`generate_plist`]. Pure string parsing so it is unit-testable: the legacy
/// plist's exact shape is asserted in the tests against [`generate_plist`]'s
/// own output, so a format drift trips CI.
///
/// Returns `None` if the plist has no `ProgramArguments` array or no
/// `<string>` inside it (a hand-edited or unexpected plist) — in which case
/// the caller falls back to removing only the plist itself.
#[allow(dead_code)]
pub(super) fn legacy_program_path_from_plist(plist_xml: &str) -> Option<PathBuf> {
    // Find the ProgramArguments array, then the first <string>…</string>.
    let after_key = plist_xml.split("<key>ProgramArguments</key>").nth(1)?;
    let array = after_key.split("<array>").nth(1)?;
    let array = array.split("</array>").next()?;
    let start = array.find("<string>")? + "<string>".len();
    let end = array[start..].find("</string>")? + start;
    let raw = array[start..end].trim();
    if raw.is_empty() {
        return None;
    }
    Some(PathBuf::from(xml_unescape(raw)))
}

/// Extract the freenet binary path embedded in a legacy auto-update wrapper
/// script. [`generate_wrapper_script`] writes the binary as `"{binary}" network`
/// / `"{binary}" update` lines; we recover it from the `network` invocation.
/// Pure string parsing, unit-tested against [`generate_wrapper_script`]'s own
/// output so a format drift trips CI.
///
/// Returns `None` if the expected invocation line isn't found (e.g. a
/// hand-written wrapper), in which case the caller removes only the wrapper
/// script and plist, not a guessed binary.
#[allow(dead_code)]
pub(super) fn legacy_binary_path_from_wrapper(wrapper_sh: &str) -> Option<PathBuf> {
    // Match the `"<binary>" network …` launch line. The path is quoted so an
    // install path containing spaces survives.
    for line in wrapper_sh.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('"') {
            continue;
        }
        let rest = &trimmed[1..];
        let Some(close) = rest.find('"') else {
            continue;
        };
        let path = &rest[..close];
        let after = rest[close + 1..].trim_start();
        if after.starts_with("network") && !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }
    None
}

/// Basename of the auto-update wrapper script `freenet service install`
/// writes (see [`wrapper_script_path`]). The migration only treats a plist's
/// `ProgramArguments` target as a removable Freenet wrapper if it has this
/// exact filename.
const LEGACY_WRAPPER_SCRIPT_NAME: &str = "freenet-service-wrapper.sh";

/// Distinctive header line [`generate_wrapper_script`] writes at the top of
/// every wrapper. Used as a content signature so we never delete a
/// hand-written or repurposed script that merely happens to be referenced by
/// an `org.freenet.node` plist.
const LEGACY_WRAPPER_SIGNATURE: &str = "# Freenet service wrapper for auto-update support.";

/// Returns true if `(basename, contents)` look like a Freenet-generated
/// auto-update wrapper script — i.e. it is safe for the migration to delete.
/// PURE so the guard is unit-testable. We require BOTH the canonical filename
/// AND the generated header signature, so a user who repurposed the legacy
/// `org.freenet.node` label to launch an unrelated script does NOT have that
/// script deleted (Codex P2, round 4 on PR #4448).
#[allow(dead_code)]
pub(super) fn is_legacy_freenet_wrapper(basename: Option<&str>, contents: &str) -> bool {
    basename == Some(LEGACY_WRAPPER_SCRIPT_NAME) && contents.contains(LEGACY_WRAPPER_SIGNATURE)
}

/// Reverse of [`xml_escape`] for the entity set that function emits. Used to
/// recover a real filesystem path from a plist `<string>` (e.g. a path with
/// `&amp;` in it).
#[allow(dead_code)]
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        // &amp; must be last so we don't double-decode the others.
        .replace("&amp;", "&")
}

/// Execute a [`LegacyMigrationPlan`] on macOS. Best-effort and idempotent:
/// each step is attempted independently and a failure (e.g. a root-owned
/// binary the user can't `rm` without `sudo`) logs a manual-cleanup hint
/// rather than aborting the whole migration or failing wrapper startup.
///
/// Returns `true` if a migration was attempted (so the caller knows the
/// legacy plist is gone and the normal first-run agent registration should
/// proceed), `false` if there was nothing to migrate.
#[cfg(target_os = "macos")]
fn run_legacy_install_migration(log_dir: &Path, plan: &LegacyMigrationPlan) -> bool {
    let LegacyMigrationPlan::Migrate(steps) = plan else {
        return false;
    };
    log_wrapper_event(
        log_dir,
        "Legacy install.sh install detected on first DMG launch; migrating to \
         the DMG-managed launch agent (issue #3943).",
    );
    for step in steps {
        match step {
            LegacyMigrationStep::BootOutAndRemovePlist(plist) => {
                // Bootout first so launchd stops auto-respawning the legacy
                // backend; bootout is a no-op/non-fatal if not loaded.
                launchctl_bootout(plist);
                match remove_launch_agent_plist_at(plist) {
                    Ok(()) => log_wrapper_event(
                        log_dir,
                        &format!("Migration: removed legacy launch agent {}", plist.display()),
                    ),
                    Err(e) => log_wrapper_event(
                        log_dir,
                        &format!(
                            "Migration: could not remove legacy launch agent {} ({e}). \
                             Please remove it manually: rm {}",
                            plist.display(),
                            plist.display()
                        ),
                    ),
                }
            }
            LegacyMigrationStep::RemoveLegacyWrapperScript(wrapper) => {
                remove_legacy_file(log_dir, "legacy wrapper script", wrapper);
            }
            LegacyMigrationStep::RemoveLegacyBinary(bin) => {
                remove_legacy_file(log_dir, "legacy CLI binary", bin);
            }
        }
    }
    true
}

/// Remove a single legacy file as part of the migration, logging the outcome.
/// Idempotent: a missing file is a no-op (it raced away between the caller's
/// existence check and now). A permission failure (e.g. a root-owned binary
/// the user can't `rm` without `sudo`) logs a manual-cleanup hint rather than
/// failing the migration.
#[cfg(target_os = "macos")]
fn remove_legacy_file(log_dir: &Path, kind: &str, path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => log_wrapper_event(
            log_dir,
            &format!("Migration: removed {kind} {}", path.display()),
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Raced away between the existence check and now; fine.
        }
        Err(e) => log_wrapper_event(
            log_dir,
            &format!(
                "Migration: could not remove {kind} {} ({e}). \
                 If it persists, remove it manually: sudo rm {}",
                path.display(),
                path.display()
            ),
        ),
    }
}

/// Resolve a legacy install's own artifacts by reading its plist and the
/// wrapper script the plist invokes. Side-effecting (file I/O) but does NOT
/// mutate anything; the pure decision still lives in
/// [`legacy_install_migration_plan`]. Only artifacts that exist on disk are
/// returned, so the planner's output matches what is actually present.
///
/// We deliberately derive the binary from the wrapper script rather than
/// guessing install locations, so we never remove an unrelated user-installed
/// `freenet`/`fdev` (Codex P2, round 1 on PR #4448). We also VALIDATE that the
/// referenced script is actually a Freenet-generated wrapper (canonical
/// filename + header signature) before scheduling it — or its binary — for
/// removal, so a hand-edited or repurposed `org.freenet.node` plist pointing
/// at an unrelated script cannot cause that script/binary to be deleted
/// (Codex P2, round 4).
#[cfg(target_os = "macos")]
fn resolve_legacy_install(legacy_plist: &Path) -> ResolvedLegacyInstall {
    let Ok(plist_xml) = std::fs::read_to_string(legacy_plist) else {
        return ResolvedLegacyInstall::default();
    };
    let Some(wrapper_path) = legacy_program_path_from_plist(&plist_xml) else {
        return ResolvedLegacyInstall::default();
    };

    // Only act on the wrapper if it is genuinely a Freenet-generated script.
    // If we can't read it, or it doesn't look like ours, remove nothing here
    // (the caller still removes the plist itself, which is the race cause).
    let Ok(wrapper_sh) = std::fs::read_to_string(&wrapper_path) else {
        return ResolvedLegacyInstall::default();
    };
    let basename = wrapper_path.file_name().and_then(|n| n.to_str());
    if !is_legacy_freenet_wrapper(basename, &wrapper_sh) {
        return ResolvedLegacyInstall::default();
    }

    // The plist's ProgramArguments points at the auto-update wrapper script,
    // not the binary directly (see `generate_plist`). Recover the binary from
    // the wrapper, then offer its sibling `fdev` too (install.sh ships both).
    // Constrain to the canonical `freenet`/`fdev` basenames as a second guard.
    let mut binaries = Vec::new();
    if let Some(bin) = legacy_binary_path_from_wrapper(&wrapper_sh) {
        if bin.file_name().and_then(|n| n.to_str()) == Some("freenet") && bin.exists() {
            if let Some(dir) = bin.parent() {
                let fdev = dir.join("fdev");
                if fdev.exists() {
                    binaries.push(fdev);
                }
            }
            binaries.push(bin);
        }
    }

    ResolvedLegacyInstall {
        wrapper_script: wrapper_path.exists().then_some(wrapper_path),
        binaries,
    }
}

/// Run all the macOS-only Launch-at-Login housekeeping that must happen
/// before the tray is built. Specifically:
///
/// - On first wrapper launch, register a user LaunchAgent so Freenet
///   auto-starts on future logins (Windows-parity auto-start).
/// - On every subsequent launch, if the user had previously enabled
///   Launch at Login but the plist's embedded executable path no longer
///   matches the current one (user moved the .app, upgraded via a new
///   DMG installed elsewhere, etc.), rewrite it.
/// - If a legacy `org.freenet.node` LaunchAgent from the install.sh era is
///   present, migrate away from it (issue #3943): boot it out, remove its
///   plist + wrapper + the binaries that install owned, then register our own
///   agent. If migration can't remove the legacy plist (rare), fall back to
///   logging a prominent manual-cleanup warning so the user knows the two
///   agents race for port 7509.
///
/// Called synchronously (not from the wrapper thread) so the tray's
/// Launch-at-Login check item reads the final filesystem state when
/// `TrayState::new` consults it. Previously the first-run registration
/// lived inside `run_wrapper_loop`, which ran on a background thread
/// AFTER the tray was built, so the menu item shipped stale-unchecked
/// on first launch even though Launch at Login had been enabled.
#[cfg(target_os = "macos")]
pub(super) fn macos_launch_at_login_startup(log_dir: &Path) {
    // Resolve the onboarding first-run marker up front: the auto-register
    // decision below uses it. (The legacy migration is gated on its OWN marker,
    // not this one — see below.)
    let Some(marker) = first_run_marker_path() else {
        return;
    };
    let is_first_run = is_first_run_at(&marker);

    // Legacy-install migration (issue #3943). If a pre-existing install.sh
    // launch agent is present, boot it out, remove its plist + auto-update
    // wrapper script, and remove the CLI binaries THAT legacy install owned,
    // so the two installs stop racing for port 7509. We resolve the legacy
    // install's own artifacts from its plist and wrapper (never guessed
    // locations) so we don't delete an unrelated `freenet`/`fdev` a user
    // installed separately. The decision is computed by the pure
    // `legacy_install_migration_plan` so it is unit-testable on Linux.
    //
    // Gated on a DEDICATED migration marker, NOT the onboarding first-run
    // marker: existing DMG users have the first-run marker set but may still
    // have the racing legacy agent, and must still get migrated (Codex P1).
    let migration_done = legacy_migration_marker_path()
        .map(|m| m.exists())
        .unwrap_or(false);
    let legacy_plist_path = launch_agent_plist_path_for(LEGACY_SERVICE_LAUNCHD_LABEL);
    let legacy_present = legacy_plist_path
        .as_ref()
        .map(|p| p.exists())
        .unwrap_or(false);
    let resolved = legacy_plist_path
        .as_ref()
        .filter(|_| !migration_done && legacy_present)
        .map(|p| resolve_legacy_install(p))
        .unwrap_or_default();
    let legacy_plist = legacy_plist_path.map(|p| (p, legacy_present));
    let plan = legacy_install_migration_plan(migration_done, legacy_plist, &resolved);
    let migrated = run_legacy_install_migration(log_dir, &plan);

    // Re-evaluate legacy presence after migration: a successful migration
    // removed the legacy plist.
    let legacy_present = if migrated {
        legacy_launchd_agent_present()
    } else {
        legacy_present
    };

    // If we migrated an existing user who had no DMG agent of their own (the
    // pre-migration code treated the legacy plist as already-enabled, so an
    // already-onboarded user never got ours), register the replacement NOW —
    // in the same launch as the migration — so they aren't left with Launch
    // at Login silently off (Codex P2, round 2).
    //
    // This is intentionally a ONE-SHOT tied to the migrating launch, NOT a
    // durable "re-register whenever our plist is absent" rule: once the
    // migration marker is written (below), a migrated user who later DISABLES
    // Launch at Login via the tray must keep it disabled — we must not
    // recreate the plist against their explicit choice (Codex P2, round 5).
    let migrated_off_legacy = migrated && !legacy_present;
    if migrated_off_legacy && !is_launch_at_login_enabled() {
        match std::env::current_exe() {
            Ok(exe) => match enable_launch_at_login(&exe) {
                Ok(()) => log_wrapper_event(
                    log_dir,
                    "Migration: registered replacement Launch at Login agent",
                ),
                Err(e) => log_wrapper_event(
                    log_dir,
                    &format!(
                        "Migration: replacement Launch at Login registration failed ({e}). \
                         Enable it from the Freenet tray menu if you want auto-start on login."
                    ),
                ),
            },
            Err(e) => log_wrapper_event(
                log_dir,
                &format!("Migration: could not resolve current exe for replacement agent: {e}"),
            ),
        }
    }

    // Mark the migration done ONLY once the legacy plist is actually gone, so
    // a launch where plist removal was denied (rare; the plist is user-owned)
    // retries next time rather than giving up permanently. The marker means
    // "the legacy race is resolved": once written we never auto-manage Launch
    // at Login off the migration path again, so a later user disable sticks
    // (Codex P2, round 5).
    if migrated && !legacy_present {
        if let Some(m) = legacy_migration_marker_path() {
            if let Err(e) = mark_first_run_complete_at(&m) {
                log_wrapper_event(
                    log_dir,
                    &format!(
                        "Migration: failed to write migration marker {}: {e}",
                        m.display()
                    ),
                );
            }
        }
    }

    // Legacy-agent warning: if the legacy plist is STILL present (we either
    // didn't migrate this launch, or removal was denied), warn the user.
    // Treating the legacy plist's presence as "already enabled" below avoids
    // layering a second auto-start on top of one we couldn't remove.
    if legacy_present {
        log_wrapper_event(
            log_dir,
            "Legacy launchd agent ~/Library/LaunchAgents/org.freenet.node.plist \
             detected. Freenet is already configured to auto-start via that \
             agent; the new DMG install will NOT create a duplicate agent. \
             To clean up the legacy one when convenient: launchctl bootout \
             gui/$UID ~/Library/LaunchAgents/org.freenet.node.plist && rm \
             ~/Library/LaunchAgents/org.freenet.node.plist",
        );
    }

    // First-run onboarding auto-register: register unless something already
    // auto-starts Freenet (our own plist, or a legacy plist we couldn't
    // remove). The post-migration replacement registration is handled above;
    // here we only handle genuine first-run and the stale-plist self-heal.
    let already_enabled = is_launch_at_login_enabled() || legacy_present;
    match first_run_launch_at_login_action(is_first_run, already_enabled) {
        FirstRunLaunchAtLoginAction::Register => match std::env::current_exe() {
            Ok(exe) => match enable_launch_at_login(&exe) {
                Ok(()) => log_wrapper_event(log_dir, "First-run: registered Launch at Login agent"),
                Err(e) => log_wrapper_event(
                    log_dir,
                    &format!("First-run Launch at Login registration failed: {e}"),
                ),
            },
            Err(e) => log_wrapper_event(
                log_dir,
                &format!("First-run Launch at Login: could not resolve current exe: {e}"),
            ),
        },
        FirstRunLaunchAtLoginAction::AlreadyEnabled | FirstRunLaunchAtLoginAction::NotFirstRun => {
            // Nothing to register; if our OWN plist is present it may be stale
            // (user moved the .app, new DMG location). Refresh it. Don't
            // resurrect a rewrite cycle on a legacy-only install (own plist
            // absent), and don't fight a user who deliberately disabled it.
            if is_launch_at_login_enabled() {
                refresh_launch_at_login_plist_if_stale();
            }
        }
    }
}

#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
pub(super) fn macos_launch_at_login_startup(_log_dir: &Path) {}

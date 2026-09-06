//! Recurrence guard: the host-clock deprecation warning stays under test in CI.
//!
//! Context (#5465, PR #5470). `wasm_runtime::tests::host_clock` is the ONLY
//! thing that distinguishes `tracing::warn!` from `tracing::debug!` at the
//! deprecation warning — the unit tests all assert on a `bool` return, which
//! cannot see a log level. That module is `#[cfg(feature = "trace")]`, because
//! it installs a `tracing` capture and `tracing_subscriber` is only a
//! dependency under that feature.
//!
//! The workspace test job passes `--no-default-features`, so the crate's own
//! `default = [..., "trace"]` is NOT what turns the feature on: the explicit
//! `--features trace,...` list is. Drop `trace` from that list — an entirely
//! plausible future edit — and the module silently stops compiling into the
//! test binary. No compile error, no failing test, green CI, and the one guard
//! that closes the round-1 blocking finding is gone.
//!
//! So the flag itself needs a guard. This is a cheap read-only check in the
//! same shape as `cross_compile_feature_split.rs` and `windows_signing_order.rs`.
//!
//! It deliberately lives OUTSIDE the `trace` gate. A pin inside the gated
//! module would vanish along with the module it protects, which is the failure
//! it exists to catch.

use std::path::PathBuf;

fn ci_yml() -> String {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace layout: crates/core/../../ should resolve")
        .to_path_buf();
    let path = workspace_root.join(".github/workflows/ci.yml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {path:?}: {e}"))
}

/// Every `cargo nextest run` command in `ci.yml`, with `\` line continuations
/// joined so a multi-line invocation reads as one string, and comment lines
/// dropped so the prose documenting this very guard cannot satisfy it.
fn nextest_commands(yml: &str) -> Vec<String> {
    let mut commands = Vec::new();
    let mut current: Option<String> = None;
    for line in yml.lines().map(str::trim) {
        if line.starts_with('#') {
            continue;
        }
        let (body, continues) = match line.strip_suffix('\\') {
            Some(body) => (body.trim_end(), true),
            None => (line, false),
        };
        match current.as_mut() {
            Some(acc) => {
                acc.push(' ');
                acc.push_str(body);
                if !continues {
                    commands.push(current.take().expect("just matched Some"));
                }
            }
            None if body.contains("cargo nextest run") => {
                if continues {
                    current = Some(body.to_string());
                } else {
                    commands.push(body.to_string());
                }
            }
            None => {}
        }
    }
    // A trailing `\` at EOF would strand the accumulator; keep it rather than
    // silently dropping the command it belongs to.
    if let Some(acc) = current {
        commands.push(acc);
    }
    commands
}

#[test]
fn the_workspace_test_job_still_enables_the_trace_feature() {
    let yml = ci_yml();
    let commands = nextest_commands(&yml);

    assert!(
        !commands.is_empty(),
        "no `cargo nextest run` commands found in ci.yml — did the workflow move \
         or change shape? This guard must be updated so it keeps protecting the \
         host-clock deprecation warning's only real test."
    );

    // The command that runs `crates/core`'s unit tests is the one that both
    // opts out of default features and names the feature list explicitly.
    let explicit: Vec<&String> = commands
        .iter()
        .filter(|c| c.contains("--no-default-features") && c.contains("--features"))
        .collect();

    assert!(
        !explicit.is_empty(),
        "no `cargo nextest run --no-default-features --features ...` command in \
         ci.yml. If the workspace test job stopped opting out of default \
         features then `trace` comes from the crate default and this guard is \
         moot — but confirm that before deleting it, because `wasm_runtime::\
         tests::host_clock` silently does not run without it.\n\
         Commands found:\n  {}",
        commands.join("\n  ")
    );

    for command in explicit {
        let features = command
            .split("--features")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .unwrap_or_else(|| {
                panic!("`--features` with no value in ci.yml command:\n  {command}")
            });
        assert!(
            features.split(',').any(|f| f == "trace"),
            "ci.yml runs the workspace tests without the `trace` feature, so \
             `wasm_runtime::tests::host_clock` is not compiled and NOTHING \
             checks that the #5465 deprecation warning is emitted at WARN \
             rather than DEBUG. Re-add `trace` to this command's feature \
             list.\n  command: {command}\n  features: {features}"
        );
    }
}

/// The guard above is only worth anything if the module it protects is still
/// gated the way it assumes. If the gate is removed the feature flag stops
/// mattering and this file should go; if the module is renamed the gate needs
/// to follow it.
#[test]
fn the_host_clock_test_module_is_still_gated_on_trace() {
    let tests_rs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/wasm_runtime/tests.rs");
    let src = std::fs::read_to_string(&tests_rs)
        .unwrap_or_else(|e| panic!("failed to read {tests_rs:?}: {e}"));
    let code: String = src
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        code.contains("#[cfg(feature = \"trace\")]\nmod host_clock;"),
        "`mod host_clock;` in wasm_runtime/tests.rs is no longer immediately \
         preceded by `#[cfg(feature = \"trace\")]`. If the gate was removed, the \
         ci.yml feature guard in this file is obsolete and should be deleted \
         with it; if the module was renamed, update both."
    );
}

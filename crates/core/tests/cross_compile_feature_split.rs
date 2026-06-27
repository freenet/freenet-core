//! Recurrence guard for the freenet/fdev build split in
//! `.github/workflows/cross-compile.yml`.
//!
//! Context (PR #4605 / the 0.2.81 telemetry blackout): `fdev` depends on
//! `freenet` with `features = ["testing"]`. If the cross-compile workflow
//! compiles `freenet` and `fdev` in the SAME `cargo build` invocation — either
//! explicitly (`-p freenet -p fdev`) or implicitly (a bare `cargo build` that
//! compiles the whole workspace, the original macOS/Windows form) — Cargo
//! resolver-v2 feature unification turns the `testing` feature ON for the
//! SHIPPED `freenet` binary. That silently disabled telemetry on every release
//! node from 0.2.81 onward (back when the suppression guard keyed on
//! `cfg!(feature = "testing")`) and pulled in deadlock-detection overhead. The
//! fix is to build `freenet` first and ALONE, then `fdev` separately.
//!
//! This is a cheap, read-only guard (no boot step, no `compile_error!`): it
//! fails CI if any `cargo build` line in the cross-compile workflow would
//! compile `freenet` together with `fdev`.

use std::path::PathBuf;

fn cross_compile_yml() -> String {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace layout: crates/core/../../ should resolve")
        .to_path_buf();
    let path = workspace_root.join(".github/workflows/cross-compile.yml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {path:?}: {e}"))
}

#[test]
fn freenet_and_fdev_build_in_separate_cargo_invocations() {
    let yml = cross_compile_yml();

    // Real build commands only: skip comment lines (YAML `#` and shell `#`
    // inside `run:` blocks both start with `#` after trimming) so the prose
    // that documents this very regression doesn't trip the guard.
    let build_lines: Vec<&str> = yml
        .lines()
        .map(str::trim)
        .filter(|l| l.contains("cargo build") && !l.starts_with('#'))
        .collect();

    assert!(
        !build_lines.is_empty(),
        "no `cargo build` command lines found in cross-compile.yml — did the workflow \
         move or change shape? This guard (the 0.2.81 telemetry blackout) must be \
         updated so it keeps protecting the freenet/fdev build split."
    );

    let mut freenet_invocations = 0usize;
    let mut fdev_invocations = 0usize;
    for line in &build_lines {
        let has_freenet = line.contains("-p freenet");
        let has_fdev = line.contains("-p fdev");

        assert!(
            !(has_freenet && has_fdev),
            "cross-compile.yml builds freenet and fdev in ONE cargo invocation:\n  {line}\n\
             A combined `-p freenet -p fdev` build unifies fdev's `freenet/testing` feature \
             onto the SHIPPED freenet binary (the 0.2.81 telemetry blackout). Build freenet \
             first and alone, then fdev separately."
        );
        assert!(
            has_freenet || has_fdev,
            "cross-compile.yml has a `cargo build` that selects neither `-p freenet` nor \
             `-p fdev`:\n  {line}\n\
             A bare / whole-workspace build compiles freenet TOGETHER with fdev, unifying \
             fdev's `freenet/testing` onto the shipped freenet binary (the 0.2.81 telemetry \
             blackout). Always build the shipped binaries with an explicit `-p freenet` \
             (alone) then `-p fdev`."
        );

        if has_freenet {
            freenet_invocations += 1;
        } else {
            fdev_invocations += 1;
        }
    }

    assert!(
        freenet_invocations > 0,
        "cross-compile.yml never builds `freenet` via a dedicated `-p freenet` invocation"
    );
    assert!(
        fdev_invocations > 0,
        "cross-compile.yml never builds `fdev` via a dedicated `-p fdev` invocation"
    );
}

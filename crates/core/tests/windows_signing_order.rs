//! Recurrence guard for the Windows Authenticode signing / checksum ordering in
//! `.github/workflows/cross-compile.yml`.
//!
//! # Why this guard exists
//!
//! Windows release binaries are Authenticode-signed in `build-x86_64-windows`,
//! and `SHA256SUMS.txt` is generated later in `attach-to-release` from the
//! artifacts that job uploads. The auto-updater
//! (`crates/core/src/bin/commands/update.rs`) authenticates `SHA256SUMS.txt`
//! against a baked-in ed25519 key and then checks each downloaded asset's
//! SHA-256 against that manifest.
//!
//! That chain is only consistent while **signing precedes upload**, because
//! signing CHANGES THE BYTES of the `.exe` (the Authenticode signature is
//! appended to the PE certificate table). Move signing after the
//! `upload-artifact` steps — or generate the checksums from anything other than
//! the artifacts the signing job produced — and the manifest describes UNSIGNED
//! bytes while the release ships SIGNED ones. Every Windows auto-update then
//! fails checksum verification.
//!
//! # Why a source-scrape guard rather than a CI job
//!
//! The auto-update canary (Gate A / Gate B) covers **Linux musl x86_64 only** —
//! see freenet/freenet-core#5341. Windows auto-update has no end-to-end gate at
//! all. So the reordering described above is a silent, total, platform-wide
//! break that **nothing else in CI would catch**, sitting one refactor away.
//! This test is the backstop for that specific hole.
//!
//! # Note for whoever edits the workflow next
//!
//! If you are here because this test failed, do not "fix" it by relaxing the
//! assertion. Put the signing steps back before the uploads. The ordering is
//! load-bearing for auto-update on a platform with no other coverage.
//!
//! The checks below are pure functions over the workflow text so that the
//! guard's own ability to FAIL is itself pinned: the synthetic-mutation tests
//! at the bottom feed deliberately-broken workflows in and assert each check
//! rejects them. A guard that cannot go red is not a guard.

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

/// Line range `[start, end)` of a top-level job block (jobs sit at indent 2).
fn job_range(yml: &str, job: &str) -> Result<(usize, usize), String> {
    let lines: Vec<&str> = yml.lines().collect();
    let header = format!("  {job}:");
    let start = lines
        .iter()
        .position(|l| *l == header)
        .ok_or_else(|| format!("job `{job}` not found in cross-compile.yml (looked for the exact line `{header}`). If the job was renamed, update this guard so it keeps protecting the Windows signing order."))?;

    // The next line at indent 2 that opens another job block ends this one.
    let end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find(|(_, l)| {
            l.starts_with("  ")
                && !l.starts_with("   ")
                && l.trim_end().ends_with(':')
                && !l.trim_start().starts_with('#')
        })
        .map(|(i, _)| i)
        .unwrap_or(lines.len());

    Ok((start, end))
}

/// Index of the single line in `range` containing `needle`, requiring exactly
/// `expected` occurrences. Fails closed: a vanished or duplicated anchor is an
/// error, never a silent pass.
fn sole_index(
    yml: &str,
    range: (usize, usize),
    needle: &str,
    expected: usize,
) -> Result<Vec<usize>, String> {
    let lines: Vec<&str> = yml.lines().collect();
    let hits: Vec<usize> = (range.0..range.1)
        .filter(|i| lines[*i].contains(needle) && !lines[*i].trim_start().starts_with('#'))
        .collect();
    if hits.len() != expected {
        return Err(format!(
            "expected {expected} non-comment occurrence(s) of `{needle}` in the job block, found {}. \
             The anchor this guard relies on moved or disappeared — update the guard rather than \
             deleting it, or the Windows signing/checksum ordering becomes unprotected.",
            hits.len()
        ));
    }
    Ok(hits)
}

/// Signing and its verification must both precede EVERY `upload-artifact` in
/// the Windows build job, so the uploaded artifacts carry the signature.
fn check_signing_precedes_upload(yml: &str) -> Result<(), String> {
    let range = job_range(yml, "build-x86_64-windows")?;

    // Anchor on the API surface (the action ref and the PowerShell cmdlet),
    // not on step `name:` strings, which are prose and get reworded.
    let sign = sole_index(yml, range, "azure/artifact-signing-action", 1)?[0];
    let verify = sole_index(yml, range, "Get-AuthenticodeSignature", 1)?[0];
    let uploads = sole_index(yml, range, "actions/upload-artifact", 2)?;

    for upload in &uploads {
        if sign > *upload {
            return Err(format!(
                "cross-compile.yml signs the Windows binaries (line {}) AFTER an \
                 upload-artifact step (line {}).\n\
                 Signing appends bytes to the PE, so the uploaded artifact would be UNSIGNED \
                 while SHA256SUMS.txt (generated later from these artifacts) would describe \
                 those unsigned bytes. Windows auto-update has no canary (#5341), so nothing \
                 else in CI catches this. Move signing back above the uploads.",
                sign + 1,
                upload + 1
            ));
        }
        if verify > *upload {
            return Err(format!(
                "cross-compile.yml verifies the Windows signatures (line {}) AFTER an \
                 upload-artifact step (line {}).\n\
                 The Get-AuthenticodeSignature gate must run BEFORE upload, or an unsigned \
                 or untimestamped binary can still reach the release.",
                verify + 1,
                upload + 1
            ));
        }
    }

    Ok(())
}

/// The Windows job must keep declaring `environment: release` and
/// `permissions: id-token: write` (plus `contents: read`).
///
/// These are the authentication contract, not decoration. The Entra federated
/// credential is pinned to the subject
/// `repo:freenet/freenet-core:environment:release`, so a job that does not
/// declare the environment cannot mint an Azure token at all. And because
/// naming ANY permission drops the rest to none, `contents: read` has to be
/// restated or `actions/checkout` loses repository access.
///
/// Dropping either would not fail here — it would fail on the next TAG push,
/// as an opaque Azure auth error, at exactly the moment a release is being
/// cut. Windows auto-update has no canary (#5341) and signing is fail-closed,
/// so that error arrives as a stuck draft release rather than as anything
/// diagnosable. Pin it where the mistake is made instead.
fn check_oidc_wiring_intact(yml: &str) -> Result<(), String> {
    let range = job_range(yml, "build-x86_64-windows")?;
    let lines: Vec<&str> = yml.lines().collect();

    let has = |needle: &str| {
        (range.0..range.1)
            .any(|i| lines[i].trim() == needle && !lines[i].trim_start().starts_with('#'))
    };

    if !has("environment: release") {
        return Err(
            "build-x86_64-windows no longer declares `environment: release`.\n\
             The Entra federated credential is pinned to the subject \
             `repo:freenet/freenet-core:environment:release`, so without this line the job \
             cannot obtain an Azure token and signing fails — as an opaque auth error on the \
             next tag push, blocking the release as a draft. This is not a simplification."
                .to_string(),
        );
    }

    if !has("id-token: write") {
        return Err(
            "build-x86_64-windows no longer requests `id-token: write`.\n\
             OIDC federation to Azure cannot work without it; azure/login will fail to mint a \
             token and Windows signing will fail on the next tag push."
                .to_string(),
        );
    }

    if !has("contents: read") {
        return Err(
            "build-x86_64-windows names permissions but no longer restates `contents: read`.\n\
             Naming ANY permission drops the rest to none, so actions/checkout loses repository \
             access and the job fails before it ever reaches the signing steps."
                .to_string(),
        );
    }

    Ok(())
}

/// Every binary the Windows job UPLOADS must also be named in the
/// `Get-AuthenticodeSignature` verification loop.
///
/// Without this, dropping one entry from that loop ships an unsigned binary
/// while the gate still passes. `fdev.exe` is the live risk: the setup wizard
/// downloads it from the release at install time, so an unsigned `fdev.exe`
/// lands next to a signed `freenet.exe` — the exact "unsigned binary beside a
/// signed one" case the signing work exists to prevent.
fn check_every_uploaded_binary_is_verified(yml: &str) -> Result<(), String> {
    let range = job_range(yml, "build-x86_64-windows")?;
    let lines: Vec<&str> = yml.lines().collect();

    // Basenames of everything the job uploads, e.g. `freenet.exe`.
    let uploaded: Vec<String> = (range.0..range.1)
        .filter(|i| {
            let t = lines[*i].trim();
            t.starts_with("path:") && !t.starts_with('#')
        })
        .filter_map(|i| {
            lines[i]
                .trim()
                .trim_start_matches("path:")
                .trim()
                .rsplit(['/', '\\'])
                .next()
                .map(str::to_string)
        })
        .collect();

    if uploaded.is_empty() {
        return Err(
            "no `path:` upload targets found in build-x86_64-windows — this guard can no longer \
             tell which binaries ship, so it cannot confirm they are all verified."
                .to_string(),
        );
    }

    // The single `foreach` line listing the paths the verification loop walks.
    let verify_list = (range.0..range.1)
        .map(|i| lines[i])
        .find(|l| l.contains("foreach") && l.contains("release"))
        .ok_or_else(|| {
            "could not find the `foreach` line of the Verify signatures step. If the verification \
             was restructured, update this guard so it still proves every uploaded binary is \
             checked."
                .to_string()
        })?;

    for bin in &uploaded {
        if !verify_list.contains(bin.as_str()) {
            return Err(format!(
                "build-x86_64-windows uploads `{bin}` but the Verify signatures loop does not \
                 check it:\n  {}\n\
                 That binary would ship unsigned while the gate still passed. `fdev.exe` in \
                 particular is downloaded by the installer at install time, so it lands next to \
                 a signed freenet.exe.",
                verify_list.trim()
            ));
        }
    }

    Ok(())
}

/// The checksum manifest must be produced from the artifacts the signing job
/// uploaded: `attach-to-release` must depend on the Windows job, download both
/// Windows artifacts, stage them, and only THEN run `sha256sum`.
fn check_checksums_cover_signed_artifacts(yml: &str) -> Result<(), String> {
    let range = job_range(yml, "attach-to-release")?;

    let needs = sole_index(yml, range, "needs:", 1)?[0];
    let needs_line = yml.lines().nth(needs).unwrap_or_default();
    if !needs_line.contains("build-x86_64-windows") {
        return Err(format!(
            "`attach-to-release` no longer declares `needs: [... build-x86_64-windows ...]`:\n  {}\n\
             Without that dependency the release can be assembled without the signed Windows \
             binaries, and the signing gate stops gating anything.",
            needs_line.trim()
        ));
    }

    // Both Windows artifacts must be downloaded before assets are staged.
    let dl_freenet = sole_index(yml, range, "binaries-x86_64-windows-freenet", 1)?[0];
    let dl_fdev = sole_index(yml, range, "binaries-x86_64-windows-fdev", 1)?[0];

    // Staging (the zip/cp of the signed exe) and then checksum generation.
    let stage = sole_index(yml, range, "freenet-x86_64-pc-windows-msvc.zip", 1)?[0];
    let checksums = sole_index(yml, range, "sha256sum", 1)?[0];

    for (label, idx) in [("freenet", dl_freenet), ("fdev", dl_fdev)] {
        if idx > stage {
            return Err(format!(
                "`attach-to-release` stages the Windows release assets (line {}) BEFORE \
                 downloading the {label} artifact (line {}). The staged asset would not be \
                 the signed binary produced by build-x86_64-windows.",
                stage + 1,
                idx + 1
            ));
        }
    }

    if checksums < stage {
        return Err(format!(
            "`attach-to-release` generates SHA256 checksums (line {}) BEFORE staging the \
             signed Windows assets (line {}).\n\
             SHA256SUMS.txt would then describe bytes other than the ones shipped, and every \
             Windows auto-update would fail checksum verification against a manifest that is \
             itself correctly ed25519-signed — a failure with no canary to catch it (#5341).",
            checksums + 1,
            stage + 1
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// The real workflow
// ---------------------------------------------------------------------------

#[test]
fn windows_signing_precedes_artifact_upload() {
    let yml = cross_compile_yml();
    if let Err(e) = check_signing_precedes_upload(&yml) {
        panic!("{e}");
    }
}

#[test]
fn windows_checksums_cover_the_signed_artifacts() {
    let yml = cross_compile_yml();
    if let Err(e) = check_checksums_cover_signed_artifacts(&yml) {
        panic!("{e}");
    }
}

#[test]
fn windows_job_keeps_its_oidc_wiring() {
    let yml = cross_compile_yml();
    if let Err(e) = check_oidc_wiring_intact(&yml) {
        panic!("{e}");
    }
}

#[test]
fn every_uploaded_windows_binary_is_signature_verified() {
    let yml = cross_compile_yml();
    if let Err(e) = check_every_uploaded_binary_is_verified(&yml) {
        panic!("{e}");
    }
}

/// Signing must stay gated to release tags and manual dispatch, and the gate
/// must be the SAME on all three steps. A login that runs without the signing
/// step (or a verify that runs without signing) is a misconfiguration that
/// shows up as a confusing auth error rather than as "signing is off".
#[test]
fn signing_steps_share_one_gating_condition() {
    let yml = cross_compile_yml();
    let range = job_range(&yml, "build-x86_64-windows").expect("windows job");
    let lines: Vec<&str> = yml.lines().collect();

    let gate =
        "if: startsWith(github.ref, 'refs/tags/v') || github.event_name == 'workflow_dispatch'";
    let gated = (range.0..range.1)
        .filter(|i| lines[*i].trim() == gate)
        .count();

    assert_eq!(
        gated, 3,
        "expected exactly 3 signing-related steps in build-x86_64-windows gated on \
         `{gate}` (azure/login, the signing action, and the Get-AuthenticodeSignature \
         verification), found {gated}. If the gate changed, make sure all three steps still \
         share it: a partially-gated set either burns signing quota on every main push or \
         fails a release with an auth error that looks like an Azure outage."
    );
}

/// The timestamp is not optional: Artifact Signing certificates are short-lived
/// (~3 days), so an untimestamped signature stops validating almost at once.
#[test]
fn signing_requests_an_rfc3161_timestamp() {
    let yml = cross_compile_yml();
    let range = job_range(&yml, "build-x86_64-windows").expect("windows job");
    let lines: Vec<&str> = yml.lines().collect();

    let has_timestamp = (range.0..range.1)
        .any(|i| lines[i].contains("timestamp-rfc3161") && !lines[i].trim_start().starts_with('#'));
    assert!(
        has_timestamp,
        "the Windows signing step no longer passes `timestamp-rfc3161`. Artifact Signing \
         certificates live ~3 days; without a countersigned timestamp every released binary \
         stops validating shortly after the cert rotates, retroactively breaking already- \
         shipped releases."
    );

    let verifies_timestamp =
        (range.0..range.1).any(|i| lines[i].contains("TimeStamperCertificate"));
    assert!(
        verifies_timestamp,
        "the Verify signatures step no longer asserts `TimeStamperCertificate`. Without that \
         assertion a silently-untimestamped signature passes the gate and ships."
    );
}

// ---------------------------------------------------------------------------
// Mutation tests: prove the guards above can actually go RED.
//
// A guard nobody has watched fail is indistinguishable from a guard that
// cannot fail. These feed deliberately-broken workflows to the same pure
// functions the real-file tests use.
// ---------------------------------------------------------------------------

/// Minimal but structurally faithful stand-in for the real workflow.
fn synthetic_workflow(sign_first: bool, checksums_last: bool) -> String {
    let sign_block = "\
      - name: Azure login (OIDC)
        uses: azure/login@v3
      - name: Sign
        uses: azure/artifact-signing-action@v2
        with:
          timestamp-rfc3161: http://timestamp.acs.microsoft.com
      - name: Verify signatures
        run: |
          foreach ($f in @('target\\release\\freenet.exe','target\\release\\fdev.exe')) {
            $sig = Get-AuthenticodeSignature $f
            if (-not $sig.TimeStamperCertificate) { throw \"x\" }
          }
";
    let upload_block = "\
      - name: Upload freenet binary
        uses: actions/upload-artifact@v7
        with:
          name: binaries-x86_64-windows-freenet
          path: target/release/freenet.exe
      - name: Upload fdev binary
        uses: actions/upload-artifact@v7
        with:
          name: binaries-x86_64-windows-fdev
          path: target/release/fdev.exe
";
    let windows_steps = if sign_first {
        format!("{sign_block}{upload_block}")
    } else {
        format!("{upload_block}{sign_block}")
    };

    let dl_block = "\
      - name: Download windows freenet
        uses: actions/download-artifact@v8
        with:
          name: binaries-x86_64-windows-freenet
      - name: Download windows fdev
        uses: actions/download-artifact@v8
        with:
          name: binaries-x86_64-windows-fdev
";
    let stage_block = "\
      - name: Prepare release assets
        run: |
          zip freenet-x86_64-pc-windows-msvc.zip freenet.exe
";
    let sums_block = "\
      - name: Generate SHA256 checksums
        run: |
          sha256sum *.zip > SHA256SUMS.txt
";
    let attach_steps = if checksums_last {
        format!("{dl_block}{stage_block}{sums_block}")
    } else {
        format!("{dl_block}{sums_block}{stage_block}")
    };

    format!(
        "jobs:\n  build-x86_64-windows:\n    runs-on: windows-latest\n    environment: release\n    permissions:\n      id-token: write\n      contents: read\n    steps:\n{windows_steps}\
         \n  attach-to-release:\n    needs: [build-x86_64-windows]\n    steps:\n{attach_steps}"
    )
}

#[test]
fn guard_accepts_a_correctly_ordered_workflow() {
    let good = synthetic_workflow(true, true);
    check_signing_precedes_upload(&good).expect("correct ordering must pass");
    check_checksums_cover_signed_artifacts(&good).expect("correct ordering must pass");
}

#[test]
fn guard_rejects_signing_moved_after_upload() {
    let bad = synthetic_workflow(false, true);
    let err = check_signing_precedes_upload(&bad)
        .expect_err("signing after upload MUST be rejected — otherwise this guard is decorative");
    assert!(
        err.contains("AFTER an upload-artifact step"),
        "unexpected rejection reason: {err}"
    );
}

#[test]
fn guard_rejects_checksums_generated_before_staging() {
    let bad = synthetic_workflow(true, false);
    let err = check_checksums_cover_signed_artifacts(&bad)
        .expect_err("checksums before staging MUST be rejected");
    assert!(
        err.contains("BEFORE staging"),
        "unexpected rejection reason: {err}"
    );
}

#[test]
fn guard_rejects_a_deleted_signing_step() {
    let bad = synthetic_workflow(true, true).replace("azure/artifact-signing-action@v2", "noop");
    check_signing_precedes_upload(&bad)
        .expect_err("a deleted signing step MUST be rejected, not silently pass");
}

#[test]
fn guard_rejects_a_deleted_verification_step() {
    let bad = synthetic_workflow(true, true).replace("Get-AuthenticodeSignature", "echo");
    check_signing_precedes_upload(&bad)
        .expect_err("a deleted verification step MUST be rejected, not silently pass");
}

#[test]
fn guard_rejects_a_dropped_windows_dependency() {
    let bad = synthetic_workflow(true, true).replace("needs: [build-x86_64-windows]", "needs: [x]");
    let err = check_checksums_cover_signed_artifacts(&bad)
        .expect_err("dropping the windows dependency MUST be rejected");
    assert!(err.contains("build-x86_64-windows"), "unexpected: {err}");
}

#[test]
fn guard_accepts_a_workflow_that_verifies_every_upload() {
    let good = synthetic_workflow(true, true);
    check_every_uploaded_binary_is_verified(&good)
        .expect("a workflow verifying both uploaded binaries must pass");
}

#[test]
fn guard_rejects_an_unverified_uploaded_binary() {
    // Drop fdev.exe from the verification loop but keep uploading it.
    let bad = synthetic_workflow(true, true).replace(
        "@('target\\release\\freenet.exe','target\\release\\fdev.exe')",
        "@('target\\release\\freenet.exe')",
    );
    let err = check_every_uploaded_binary_is_verified(&bad).expect_err(
        "an uploaded-but-unverified binary MUST be rejected — this is the fdev.exe case",
    );
    assert!(
        err.contains("fdev.exe"),
        "unexpected rejection reason: {err}"
    );
}

#[test]
fn guard_rejects_a_removed_verification_loop() {
    let bad = synthetic_workflow(true, true).replace("foreach", "noop");
    check_every_uploaded_binary_is_verified(&bad)
        .expect_err("a removed verification loop MUST fail closed");
}

#[test]
fn guard_accepts_intact_oidc_wiring() {
    let good = synthetic_workflow(true, true);
    check_oidc_wiring_intact(&good).expect("intact OIDC wiring must pass");
}

#[test]
fn guard_rejects_a_dropped_release_environment() {
    let bad = synthetic_workflow(true, true).replace("    environment: release\n", "");
    let err = check_oidc_wiring_intact(&bad)
        .expect_err("dropping `environment: release` MUST be rejected — it breaks Azure auth");
    assert!(err.contains("environment: release"), "unexpected: {err}");
}

#[test]
fn guard_rejects_a_dropped_id_token_permission() {
    let bad = synthetic_workflow(true, true).replace("      id-token: write\n", "");
    let err = check_oidc_wiring_intact(&bad)
        .expect_err("dropping `id-token: write` MUST be rejected — OIDC cannot work without it");
    assert!(err.contains("id-token: write"), "unexpected: {err}");
}

#[test]
fn guard_rejects_a_dropped_contents_read_permission() {
    let bad = synthetic_workflow(true, true).replace("      contents: read\n", "");
    let err = check_oidc_wiring_intact(&bad).expect_err(
        "dropping `contents: read` MUST be rejected — naming any permission zeroes the rest",
    );
    assert!(err.contains("contents: read"), "unexpected: {err}");
}

#[test]
fn guard_rejects_a_renamed_job() {
    let bad = synthetic_workflow(true, true).replace("  build-x86_64-windows:", "  build-win:");
    check_signing_precedes_upload(&bad)
        .expect_err("a renamed job MUST fail closed rather than silently skipping the check");
}

//! `fdev conformance`: run the contract conformance verifier offline (RFC #5320).
//!
//! This is a thin CLI wrapper around `freenet::conformance` and does not
//! reimplement any of the laws itself. That is the whole point: `fdev
//! conformance` and the network's own conformance checker call the *same*
//! [`verify_case`], so a developer-facing check that could disagree with the
//! network-facing one would be worse than no check at all.
//!
//! # Usage
//!
//! ```text
//! # Check a contract directly against a handful of observed states
//! fdev conformance --wasm contract.wasm --state s1.bin --state s2.bin
//!
//! # Replay a bundle captured from the network (or an earlier `fdev` run)
//! fdev conformance --bundle observed.bin
//! ```

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Context;
use freenet::conformance::generator::Corpus;
use freenet::conformance::verifier::Bytes;
use freenet::conformance::{
    ConformanceCase, ConformanceEvidence, ConformanceProperty, GeneratorConfig, Inconclusive,
    MinimizeConfig, OracleBuildError, PropertyOutcome, ReplayBundle, RuntimeOracle, Severity,
    generate_cases, minimize, verify_case,
};
use freenet_stdlib::prelude::{CodeHash, ContractCode, ContractInstanceId};
use serde::Serialize;

/// Run the contract conformance verifier (RFC #5320) against a contract's WASM.
///
/// Runs the *exact* checks the network's own conformance checker runs; see
/// `freenet::conformance` for why the two must never be allowed to drift.
/// Diagnostics (efficiency reports, e.g. a self-delta as large as the state
/// it is against) never fail this command — only an enforceable
/// (`Severity::Violation`) merge-law break does.
#[derive(clap::Parser, Clone)]
pub struct ConformanceConfig {
    /// Path to the compiled contract WASM. Required unless `--bundle` supplies
    /// its own code; when both are given, this overrides the bundle's code.
    #[arg(long)]
    pub(crate) wasm: Option<PathBuf>,

    /// Path to the contract's parameters file. Defaults to empty parameters.
    /// Ignored when `--bundle` is given: the bundle carries its own parameters.
    #[arg(long)]
    pub(crate) params: Option<PathBuf>,

    /// A state observed for this contract. Repeat for multiple states.
    /// Required unless `--bundle` is given.
    #[arg(long = "state")]
    pub(crate) states: Vec<PathBuf>,

    /// Replay a corpus captured earlier (see `freenet::conformance::bundle`)
    /// instead of building one from `--state`.
    #[arg(long)]
    pub(crate) bundle: Option<PathBuf>,

    /// Directory of contract WASM files to resolve a bundle's code from,
    /// typically a node's `contracts` data directory.
    ///
    /// A capture bundle records which contract it belongs to by code hash and
    /// deliberately does not embed the WASM, so a corpus of many contracts stays
    /// small. Without this, replaying such a bundle means finding the matching
    /// WASM by hand, which is the whole capture-and-replay loop's weakest link.
    #[arg(long)]
    pub(crate) contract_store: Option<PathBuf>,

    /// Upper bound on how many cases to generate and check. Defaults to the
    /// generator's own default (currently 512).
    #[arg(long)]
    pub(crate) max_cases: Option<usize>,

    /// Restrict which laws to check, by name (e.g. `state_commutativity`).
    /// Repeat for multiple. Defaults to every property.
    #[arg(long = "property")]
    pub(crate) properties: Vec<String>,

    /// Emit a machine-readable JSON report instead of the human-readable one.
    #[arg(long)]
    pub(crate) json: bool,

    /// Write a bincode-encoded evidence file per distinct enforceable
    /// violation into this directory (created if it does not exist).
    #[arg(long = "evidence-out")]
    pub(crate) evidence_out: Option<PathBuf>,

    /// Save the corpus that was checked as a replay bundle at this path.
    ///
    /// Makes a run reproducible by someone else: the bundle carries the contract
    /// code, its parameters and every state checked, and `--bundle` replays it
    /// exactly. Useful for attaching a reproducer to a bug report, and for turning a
    /// one-off finding into a permanent regression corpus.
    #[arg(long = "bundle-out")]
    pub(crate) bundle_out: Option<PathBuf>,
}

pub async fn conformance(config: ConformanceConfig) -> anyhow::Result<()> {
    let properties = parse_properties(&config.properties)?;
    let (wasm, parameters, corpus) = load_inputs(&config)?;

    if corpus.is_empty() {
        anyhow::bail!("no states to check: the corpus is empty");
    }

    let mut generator_config = GeneratorConfig::default();
    if let Some(max_cases) = config.max_cases {
        generator_config.max_cases = max_cases;
    }
    if !properties.is_empty() {
        generator_config.properties = properties;
    }

    let cases = generate_cases(&corpus, &generator_config);
    // A run that checked nothing must not report success. Restricting to a property
    // whose inputs the corpus cannot supply (commutativity with one state, delta
    // idempotence with no captured deltas) otherwise prints "0 cases run" and exits
    // 0, which any automation reads as "this contract passed".
    if cases.is_empty() {
        anyhow::bail!(
            "no cases could be generated from this corpus: {} state(s), {} delta(s), \
             {} summary/summaries for the selected properties. Nothing was checked, \
             so this is not a pass — supply more states (commutativity and \
             reconciliation need at least two), or captured deltas for the \
             delta properties.",
            corpus.states.len(),
            corpus.deltas.len(),
            corpus.summaries.len(),
        );
    }

    if let Some(path) = &config.bundle_out {
        write_bundle(path, &wasm, &parameters, &corpus)?;
    }

    let mut oracle = RuntimeOracle::standalone(wasm, parameters.clone())
        .await
        .map_err(describe_oracle_build_error)?;
    let instance = oracle.instance_id();

    let outcomes: Vec<(ConformanceCase, PropertyOutcome)> = cases
        .into_iter()
        .map(|case| {
            let outcome = verify_case(&mut oracle, &case);
            (case, outcome)
        })
        .collect();

    let evidence = match &config.evidence_out {
        Some(dir) => Some(write_evidence(
            dir,
            instance,
            &parameters,
            &outcomes,
            &mut oracle,
            &corpus.states,
        )?),
        None => None,
    };

    let report = Report::build(&corpus, &outcomes, evidence);
    if config.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        report.print_human();
    }

    let just_outcomes: Vec<&PropertyOutcome> = outcomes.iter().map(|(_, o)| o).collect();
    let enforceable = just_outcomes
        .iter()
        .filter(|o| o.is_enforceable_violation())
        .count();
    if exit_code(just_outcomes) != 0 {
        // Return rather than `process::exit`, so every destructor runs. Exiting here
        // skipped the oracle's `TempDir`, leaving a scratch contract database and
        // freshly generated secrets on disk after every failing run — and failing
        // runs are the ones a developer repeats.
        //
        // A distinct error type also lets `main` give conformance violations their
        // own exit code, so CI can tell "this contract breaks a merge law" apart
        // from "the harness could not run".
        return Err(ConformanceViolations { count: enforceable }.into());
    }
    Ok(())
}

/// Save the corpus under check as a replay bundle.
///
/// Embeds the contract code, so the bundle names the contract it came from and
/// `ReplayBundle::resolve_code` can verify identity on the way back in. A bundle
/// that did not identify its contract could be replayed against an unrelated WASM,
/// which produces confident-looking results about nothing.
fn write_bundle(
    path: &Path,
    wasm: &[u8],
    parameters: &[u8],
    corpus: &Corpus,
) -> anyhow::Result<()> {
    let mut bundle = ReplayBundle::new(wasm.to_vec(), parameters.to_vec());
    bundle.states = corpus.states.iter().map(|s| s.to_vec()).collect();
    bundle.deltas = corpus.deltas.iter().map(|d| d.to_vec()).collect();
    bundle.summaries = corpus.summaries.iter().map(|s| s.to_vec()).collect();
    bundle.note = Some(format!(
        "captured by fdev conformance {}",
        env!("CARGO_PKG_VERSION")
    ));
    bundle
        .write_to(path)
        .with_context(|| format!("writing bundle to {}", path.display()))?;
    eprintln!(
        "wrote replay bundle to {} ({} state(s), {} delta(s), {} summary/summaries)",
        path.display(),
        bundle.states.len(),
        bundle.deltas.len(),
        bundle.summaries.len(),
    );
    Ok(())
}

/// The contract broke a merge law. Distinct from any harness failure, so a caller
/// can tell "this contract is unsound" apart from "the check could not run" — a
/// distinction CI needs and a single exit code cannot express.
#[derive(Debug, thiserror::Error)]
#[error("{count} enforceable conformance violation(s) found")]
pub struct ConformanceViolations {
    pub count: usize,
}

/// 0 when no [`PropertyOutcome`] is an enforceable violation, 1 otherwise.
///
/// Diagnostics (`Severity::Diagnostic`) never fail the exit code — only a
/// `Severity::Violation` finding does. Split out from [`conformance`] so it
/// can be unit tested without spinning up a WASM runtime.
fn exit_code<'a>(outcomes: impl IntoIterator<Item = &'a PropertyOutcome>) -> i32 {
    if outcomes
        .into_iter()
        .any(PropertyOutcome::is_enforceable_violation)
    {
        1
    } else {
        0
    }
}

/// Parse `--property` names against [`ConformanceProperty::ALL`].
///
/// Returns an empty vec (meaning "every property") when `names` is empty, so
/// callers can use the result directly as an override of
/// [`GeneratorConfig::properties`] without a separate emptiness check.
fn parse_properties(names: &[String]) -> anyhow::Result<Vec<ConformanceProperty>> {
    names
        .iter()
        .map(|name| {
            ConformanceProperty::ALL
                .iter()
                .copied()
                .find(|p| p.as_str() == name)
                .ok_or_else(|| {
                    let valid = ConformanceProperty::ALL
                        .iter()
                        .map(|p| p.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    anyhow::anyhow!("unknown property {name:?}; valid properties: {valid}")
                })
        })
        .collect()
}

/// Find the WASM a bundle names, by looking it up in a node's contract store.
///
/// Addresses the store exactly as the node does: the file is named by the code
/// hash's own encoding, and its contents are a VERSIONED encoding rather than raw
/// WASM — a version header precedes the code, and only the code is hashed. So
/// reading the file's bytes directly and hashing them can never match, and that
/// failure is quiet: it looks exactly like "this node never hosted that
/// contract". `ContractStore::store_contract` writes the file;
/// `ContractCode::load_versioned_from_path` is its matching reader.
fn find_code_in_store(store: &Path, bundle: &ReplayBundle) -> anyhow::Result<Vec<u8>> {
    let Some(hash) = bundle.code_hash else {
        anyhow::bail!(
            "bundle names no contract (no code hash), so no store lookup could \
             identify the right WASM"
        );
    };

    let path = store
        .join(CodeHash::new(hash).encode())
        .with_extension("wasm");
    if !path.exists() {
        anyhow::bail!(
            "this node's contract store has no code for the bundle's contract \
             (looked for {}). A peer only stores contracts it hosts, so a capture \
             replayed on a different node may need --wasm.",
            path.display()
        );
    }

    let (code, _version) = ContractCode::load_versioned_from_path(&path)
        .with_context(|| format!("reading contract code from {}", path.display()))?;
    Ok(code.data().to_vec())
}

/// Load the WASM, parameters and corpus, either from `--bundle` or from
/// `--wasm` / `--params` / `--state`.
fn load_inputs(config: &ConformanceConfig) -> anyhow::Result<(Vec<u8>, Vec<u8>, Corpus)> {
    if let Some(bundle_path) = &config.bundle {
        let bundle = ReplayBundle::read_from(bundle_path)
            .with_context(|| format!("reading bundle {}", bundle_path.display()))?;
        // Identity is checked inside the bundle, for both embedded and supplied
        // code. Replaying a corpus against the wrong contract is worse than not
        // replaying it: the run looks authoritative and means nothing, whether it
        // reports findings or a clean bill of health.
        let supplied = match (&config.wasm, &config.contract_store) {
            (Some(path), _) => Some(read_file(path)?),
            (None, Some(store)) => Some(find_code_in_store(store, &bundle)?),
            (None, None) => None,
        };
        let wasm = bundle.resolve_code(supplied).with_context(|| {
            format!(
                "resolving contract code for bundle {}",
                bundle_path.display()
            )
        })?;
        let parameters = bundle.parameters.clone();
        let corpus = bundle.to_corpus();
        Ok((wasm, parameters, corpus))
    } else {
        let wasm_path = config
            .wasm
            .as_ref()
            .context("--wasm is required unless --bundle is given")?;
        let wasm = read_file(wasm_path)?;
        let parameters = match &config.params {
            Some(path) => read_file(path)?,
            None => Vec::new(),
        };
        if config.states.is_empty() {
            anyhow::bail!("at least one --state is required unless --bundle is given");
        }
        let mut states = Vec::with_capacity(config.states.len());
        for path in &config.states {
            states.push(read_file(path)?);
        }
        let corpus = Corpus::from_states(states).deduplicated();
        Ok((wasm, parameters, corpus))
    }
}

fn read_file(path: &PathBuf) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("reading {}", path.display()))
}

/// Give the two `OracleBuildError` halves distinct, actionable messages: a
/// contract whose WASM fails to load is the contract author's problem; a
/// scratch-directory or storage-backend failure is this machine's problem,
/// not the contract's.
fn describe_oracle_build_error(err: OracleBuildError) -> anyhow::Error {
    match &err {
        OracleBuildError::Runtime(_) => {
            anyhow::anyhow!("the contract WASM failed to load into the verifier runtime: {err}")
        }
        OracleBuildError::Scratch(_) | OracleBuildError::Storage(_) => {
            anyhow::anyhow!("could not set up the local verifier environment: {err}")
        }
    }
}

/// Write one evidence file per distinct enforceable violation.
///
/// "Distinct" is by [`ConformanceEvidence::id`] (a content hash of the
/// inputs), so cases whose inputs coincide overwrite the same filename
/// instead of piling up duplicate files for one finding.
fn write_evidence(
    dir: &PathBuf,
    instance: ContractInstanceId,
    parameters: &[u8],
    outcomes: &[(ConformanceCase, PropertyOutcome)],
    oracle: &mut RuntimeOracle,
    candidates: &[Bytes],
) -> anyhow::Result<EvidenceSummary> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating evidence directory {}", dir.display()))?;

    let mut written = HashSet::new();
    let mut oversized = 0usize;
    let mut shrunk_from = 0usize;
    let mut shrunk_to = 0usize;
    for (case, outcome) in outcomes {
        if !outcome.is_enforceable_violation() {
            continue;
        }

        // Shrink before serializing. A case generated from large states can carry
        // several MB, well over the evidence size bound, and evidence that every
        // recipient rejects is not evidence. Shrinking also makes the file a usable
        // bug report rather than two large blobs that happen to disagree.
        let (minimized, shrink) = minimize(oracle, case, candidates, &MinimizeConfig::default());
        shrunk_from += shrink.original_bytes;
        shrunk_to += shrink.final_bytes;
        let observed = verify_case(oracle, &minimized).violation().cloned();
        let evidence =
            ConformanceEvidence::new(instance, parameters.to_vec(), &minimized, observed);

        // Bounds-check with the same function a receiving peer uses, so this command
        // cannot report having written evidence that no peer would accept.
        if let Err(rejected) = evidence.check_bounds() {
            oversized += 1;
            eprintln!(
                "warning: a {} finding could not be reduced to a shippable size ({rejected}); \
                 no evidence file written for it",
                minimized.property
            );
            continue;
        }

        let id = evidence.id();
        if !written.insert(id) {
            continue;
        }
        let bytes =
            bincode::serialize(&evidence).with_context(|| format!("encoding evidence {id}"))?;
        let path = dir.join(format!("{id}.bin"));
        std::fs::write(&path, bytes)
            .with_context(|| format!("writing evidence to {}", path.display()))?;
    }

    Ok(EvidenceSummary {
        directory: dir.display().to_string(),
        files_written: written.len(),
        findings_too_large: oversized,
        input_bytes_before_shrinking: shrunk_from,
        input_bytes_after_shrinking: shrunk_to,
    })
}

#[derive(Serialize)]
struct EvidenceSummary {
    directory: String,
    files_written: usize,
    /// Findings that stayed over the evidence size bound even after shrinking.
    /// Reported rather than swallowed: they are real findings that simply cannot be
    /// propagated, and silently writing nothing would look like there were none.
    findings_too_large: usize,
    /// Total case input bytes before and after minimisation, summed over the
    /// findings written. The ratio is the honest measure of whether shrinking is
    /// earning its keep, and it is what decides whether a finding fits in evidence
    /// at all.
    input_bytes_before_shrinking: usize,
    input_bytes_after_shrinking: usize,
}

#[derive(Serialize)]
struct Report {
    /// What the cases were drawn FROM.
    ///
    /// Without this, a clean run is uninterpretable: "400 cases held" reads the
    /// same whether those cases came from thirty distinct states or from three.
    /// The case count measures work done, not coverage, and a corpus of two
    /// states can fill hundreds of cases by permuting the same pair. A reader
    /// deciding how much a clean result is worth needs the denominator.
    corpus_states: usize,
    corpus_deltas: usize,
    corpus_summaries: usize,
    cases_run: usize,
    holds: usize,
    violations: usize,
    enforceable_violations: usize,
    diagnostic_violations: usize,
    inconclusive: usize,
    findings: Vec<Finding>,
    inconclusive_reasons: Vec<InconclusiveReason>,
    evidence: Option<EvidenceSummary>,
}

/// One violated case, at full per-case granularity: exactly the digests that
/// case produced. `--json` reports these as-is; the human view groups them
/// (see [`group_findings`]) since two cases of the same defect never share
/// digests (different inputs, same law broken).
#[derive(Serialize, Clone)]
struct Finding {
    property: String,
    severity: &'static str,
    detail: String,
    left: String,
    right: String,
}

#[derive(Serialize)]
struct InconclusiveReason {
    reason: &'static str,
    occurrences: usize,
}

impl Report {
    /// Build a report from the raw per-case outcomes. `findings` keeps full
    /// per-case granularity (real digests per case) so `--json` never loses
    /// information; deduplication for the human view happens at print time,
    /// in [`group_findings`].
    fn build(
        corpus: &Corpus,
        outcomes: &[(ConformanceCase, PropertyOutcome)],
        evidence: Option<EvidenceSummary>,
    ) -> Self {
        let mut holds = 0usize;
        let mut violations = 0usize;
        let mut enforceable_violations = 0usize;
        let mut diagnostic_violations = 0usize;
        let mut inconclusive = 0usize;

        let mut findings: Vec<Finding> = Vec::new();
        let mut inconclusive_reasons: HashMap<&'static str, usize> = HashMap::new();

        for (_, outcome) in outcomes {
            match outcome {
                PropertyOutcome::Holds => holds += 1,
                PropertyOutcome::Violated(v) => {
                    violations += 1;
                    match v.severity {
                        Severity::Violation => enforceable_violations += 1,
                        Severity::Diagnostic => diagnostic_violations += 1,
                    }
                    findings.push(Finding {
                        property: v.property.as_str().to_string(),
                        severity: match v.severity {
                            Severity::Violation => "violation",
                            Severity::Diagnostic => "diagnostic",
                        },
                        detail: v.detail.clone(),
                        left: v.left.to_string(),
                        right: v.right.to_string(),
                    });
                }
                PropertyOutcome::Inconclusive(reason) => {
                    inconclusive += 1;
                    *inconclusive_reasons
                        .entry(inconclusive_label(reason))
                        .or_insert(0) += 1;
                }
            }
        }

        let mut inconclusive_reasons: Vec<InconclusiveReason> = inconclusive_reasons
            .into_iter()
            .map(|(reason, occurrences)| InconclusiveReason {
                reason,
                occurrences,
            })
            .collect();
        inconclusive_reasons.sort_by(|a, b| b.occurrences.cmp(&a.occurrences));

        Report {
            corpus_states: corpus.states.len(),
            corpus_deltas: corpus.deltas.len(),
            corpus_summaries: corpus.summaries.len(),
            cases_run: outcomes.len(),
            holds,
            violations,
            enforceable_violations,
            diagnostic_violations,
            inconclusive,
            findings,
            inconclusive_reasons,
            evidence,
        }
    }

    fn print_human(&self) {
        println!(
            "conformance: {} state(s), {} delta(s), {} summary/summaries in the corpus",
            self.corpus_states, self.corpus_deltas, self.corpus_summaries
        );
        println!(
            "conformance: {} case(s) run \u{2014} {} held, {} violation(s) ({} enforceable, {} diagnostic-only), {} inconclusive",
            self.cases_run,
            self.holds,
            self.violations,
            self.enforceable_violations,
            self.diagnostic_violations,
            self.inconclusive
        );

        if !self.findings.is_empty() {
            println!("\nfindings:");
            for (f, count) in group_findings(&self.findings) {
                let cases = if count == 1 {
                    "1 case".to_string()
                } else {
                    format!("{count} cases")
                };
                println!(
                    "  [{}] {} ({cases}): {}\n      example \u{2014} left: {}; right: {}",
                    f.severity, f.property, f.detail, f.left, f.right
                );
            }
        }

        // Inconclusive results are not failures. A contract that legitimately
        // rejects updates or requires related state produces many of these,
        // and printing them as errors would train an author to "fix" working
        // code.
        if !self.inconclusive_reasons.is_empty() {
            println!(
                "\ninconclusive ({} total, not failures \u{2014} see freenet::conformance::Inconclusive):",
                self.inconclusive
            );
            for r in &self.inconclusive_reasons {
                println!("  {}: {}", r.reason, r.occurrences);
            }
        }

        if let Some(evidence) = &self.evidence {
            println!(
                "\nwrote {} evidence file(s) to {}",
                evidence.files_written, evidence.directory
            );
        }

        if self.enforceable_violations == 0 {
            println!("\nno enforceable violations found.");
            if self.diagnostic_violations > 0 {
                println!(
                    "({} diagnostic finding(s) above are efficiency notes, not merge-law breaks, and do not fail this command.)",
                    self.diagnostic_violations
                );
            }
        }
    }
}

/// Group findings for the human-readable view.
///
/// Same property + detail is the same underlying defect, even though the
/// digests differ per case: each case's inputs are different (different
/// states drawn from the corpus), so keying the grouping on the digests
/// (the original bug here) never collapses anything — a last-write-wins
/// contract against a real corpus reported one near-identical line per
/// case instead of one line with a count. The grouping key deliberately
/// excludes `left`/`right`; the returned tuple keeps one representative
/// finding (the first seen) plus how many cases produced it.
fn group_findings(findings: &[Finding]) -> Vec<(&Finding, usize)> {
    let mut groups: Vec<(&Finding, usize)> = Vec::new();
    for f in findings {
        match groups
            .iter_mut()
            .find(|(g, _)| g.property == f.property && g.detail == f.detail)
        {
            Some((_, count)) => *count += 1,
            None => groups.push((f, 1)),
        }
    }
    groups
}

/// A stable, non-exhaustive-safe label for an [`Inconclusive`] reason.
///
/// `Inconclusive` is `#[non_exhaustive]`, so matching it from this crate
/// requires a wildcard arm regardless of how many variants are listed. The
/// wildcard still counts the occurrence (under "other") rather than
/// dropping it, so a future variant shows up in the summary instead of
/// silently vanishing.
fn inconclusive_label(reason: &Inconclusive) -> &'static str {
    match reason {
        Inconclusive::InputNotValid => "input not valid",
        Inconclusive::RelatedRequired => "requires related contract state",
        Inconclusive::ContractError(_) => "contract error",
        Inconclusive::NoOutputState => "update produced no output state",
        Inconclusive::ResourceLimit(_) => "resource limit hit",
        Inconclusive::RoundLimit => "reconciliation round budget exhausted",
        Inconclusive::MalformedCase(_) => "malformed case",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet::conformance::Violation;

    /// A contract store holds a VERSIONED encoding, not raw WASM: a header
    /// precedes the code. Resolving a bundle against it therefore has two ways to
    /// fail quietly — addressing the file by the wrong name, and reading its bytes
    /// without stripping the header — and both look identical to "this node never
    /// hosted that contract".
    ///
    /// So this writes a store entry the way the node writes one and asserts the
    /// code comes back byte-for-byte. Reading the file raw returns the header too
    /// and fails here, which is verified by mutation.
    #[test]
    fn code_is_resolved_from_a_contract_store_the_way_the_node_writes_it() {
        let store = tempfile::tempdir().expect("temp dir");
        // Not valid WASM, and it does not need to be: resolution is addressing
        // plus decoding, and nothing here compiles the module.
        let code = b"\0asm-not-really-but-bytes-are-bytes".to_vec();

        let encoded = ContractCode::from(code.clone())
            .to_bytes_versioned(freenet_stdlib::prelude::APIVersion::Version0_0_1)
            .expect("encode versioned");
        let path = store
            .path()
            .join(CodeHash::from_code(&code).encode())
            .with_extension("wasm");
        std::fs::write(&path, &encoded).expect("write store entry");
        assert!(
            encoded.len() > code.len(),
            "the stored form must carry a header, or this test proves nothing \
             about stripping one"
        );

        let bundle = ReplayBundle::new(code.clone(), Vec::new());
        let resolved = find_code_in_store(store.path(), &bundle).expect("resolve from store");
        assert_eq!(resolved, code, "resolved code must be the raw WASM");

        // A contract the node never stored must say so rather than return
        // something wrong: that is the common case when a capture is replayed on
        // a different peer.
        let absent = ReplayBundle::new(b"different code entirely".to_vec(), Vec::new());
        let err = find_code_in_store(store.path(), &absent)
            .expect_err("a contract absent from the store must not resolve");
        assert!(
            err.to_string()
                .contains("no code for the bundle's contract"),
            "error should name the miss, got: {err}"
        );
    }

    /// Every `ConformanceProperty::ALL` variant must round-trip through
    /// `as_str()` -> `parse_properties()`, so a property added later cannot
    /// silently become unparseable from the CLI.
    #[test]
    fn every_property_name_round_trips() {
        for property in ConformanceProperty::ALL {
            let parsed = parse_properties(&[property.as_str().to_string()])
                .unwrap_or_else(|e| panic!("{}: {e}", property.as_str()));
            assert_eq!(parsed, vec![*property]);
        }
    }

    #[test]
    fn unknown_property_name_lists_valid_names_in_error() {
        let err = parse_properties(&["not_a_real_property".to_string()])
            .expect_err("unknown property name should fail to parse");
        let message = err.to_string();
        assert!(message.contains("not_a_real_property"));
        // Spot check one real name is listed to help the author fix the typo.
        assert!(message.contains(ConformanceProperty::StateCommutativity.as_str()));
    }

    fn digest() -> freenet::conformance::OutputDigest {
        freenet::conformance::OutputDigest::of(b"x")
    }

    fn violation(severity: Severity) -> Violation {
        Violation {
            property: ConformanceProperty::StateCommutativity,
            severity,
            left: digest(),
            right: digest(),
            detail: "test".to_string(),
        }
    }

    /// The exit-code decision is the enforcement boundary: a diagnostic-only
    /// finding (e.g. the wasteful-self-delta checks) must never fail the
    /// command, only an enforceable `Severity::Violation` finding may.
    #[test]
    fn exit_code_treats_diagnostic_violation_as_success() {
        let outcome = PropertyOutcome::Violated(violation(Severity::Diagnostic));
        assert_eq!(exit_code([&outcome]), 0);
    }

    #[test]
    fn exit_code_treats_enforceable_violation_as_failure() {
        let outcome = PropertyOutcome::Violated(violation(Severity::Violation));
        assert_eq!(exit_code([&outcome]), 1);
    }

    #[test]
    fn exit_code_is_zero_for_holds_and_inconclusive() {
        let holds = PropertyOutcome::Holds;
        let inconclusive = PropertyOutcome::Inconclusive(Inconclusive::RoundLimit);
        assert_eq!(exit_code([&holds, &inconclusive]), 0);
    }

    fn finding(property: &str, detail: &str, left: &str, right: &str) -> Finding {
        Finding {
            property: property.to_string(),
            severity: "violation",
            detail: detail.to_string(),
            left: left.to_string(),
            right: right.to_string(),
        }
    }

    /// Regression pin: the original grouping keyed on the output digests,
    /// which differ per case, so a real corpus never actually collapsed
    /// anything — every case of the same defect printed its own line. Two
    /// findings with the same property and detail but different digests
    /// (exactly what two different cases of the same broken merge produce)
    /// must collapse into one group with count 2.
    #[test]
    fn group_findings_collapses_same_property_and_detail_with_different_digests() {
        let a = finding(
            "state_commutativity",
            "merge(A, B) must equal merge(B, A)",
            "2 bytes, blake3:aaaaaa",
            "2 bytes, blake3:bbbbbb",
        );
        let b = finding(
            "state_commutativity",
            "merge(A, B) must equal merge(B, A)",
            "1 bytes, blake3:cccccc",
            "2 bytes, blake3:dddddd",
        );
        let findings = vec![a, b];
        let groups = group_findings(&findings);
        assert_eq!(
            groups.len(),
            1,
            "same property + detail must collapse into one group regardless of digests"
        );
        assert_eq!(groups[0].1, 2);
    }

    #[test]
    fn group_findings_keeps_distinct_property_or_detail_separate() {
        let a = finding("state_commutativity", "detail A", "l1", "r1");
        let b = finding("state_associativity", "detail A", "l2", "r2");
        let c = finding("state_commutativity", "detail B", "l3", "r3");
        let findings = vec![a, b, c];
        let groups = group_findings(&findings);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|(_, count)| *count == 1));
    }
}

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
    ConformanceCase, ConformanceEvidence, ConformanceProperty, EVIDENCE_SCHEMA_VERSION,
    GeneratorConfig, Inconclusive, MinimizeConfig, OracleBuildError, PropertyOutcome, ReplayBundle,
    RuntimeOracle, Severity, generate_cases, minimize, verify_case,
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

    /// Verify an evidence file someone else produced, instead of checking a
    /// contract from scratch.
    ///
    /// This is the receiving half of the RFC's evidence model, and the half that
    /// makes the model worth anything: evidence ships INPUTS, never a verdict, and
    /// the recipient re-executes and reaches its own conclusion. Without a way to
    /// consume one, a shipped finding has to be taken on trust — which is exactly
    /// what the design refuses to do.
    ///
    /// Needs the contract, via `--wasm` or `--contract-store`. Evidence
    /// deliberately does not carry code: a peer receiving it already hosts the
    /// contract, and embedding WASM in every finding would make findings
    /// expensive to pass around.
    #[arg(long = "evidence")]
    pub(crate) evidence_in: Option<PathBuf>,

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
    if let Some(path) = config.evidence_in.clone() {
        return verify_evidence(&config, &path).await;
    }
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
            &corpus.deltas,
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

/// Re-check a finding someone else shipped, and reach an independent conclusion.
///
/// The one rule here is that `evidence.observed` is never trusted. It records what
/// the discovering peer believed and exists for diagnostics only; the verdict this
/// prints comes from re-executing the inputs locally. A recipient that believed the
/// sender would let any peer accuse any contract, which is the failure the whole
/// ship-inputs-not-verdicts design exists to prevent.
///
/// Disagreement is therefore a real outcome and is reported as one rather than
/// being smoothed over: it means either the sender was wrong or the two runtimes
/// differ, and both are worth knowing.
async fn verify_evidence(config: &ConformanceConfig, path: &PathBuf) -> anyhow::Result<()> {
    let bytes = read_file(path)?;
    let evidence: ConformanceEvidence = bincode::deserialize(&bytes)
        .with_context(|| format!("decoding evidence {}", path.display()))?;

    // Refuse a schema this build does not know, rather than reading fields that may
    // have changed meaning. Silently misreading evidence is worse than declining.
    if evidence.schema_version != EVIDENCE_SCHEMA_VERSION {
        anyhow::bail!(
            "evidence uses schema version {}, this build understands {}",
            evidence.schema_version,
            EVIDENCE_SCHEMA_VERSION
        );
    }

    // Bounds-check with the same function a receiving peer uses, so this command
    // never accepts evidence the network itself would reject.
    evidence
        .check_bounds()
        .map_err(|rejected| anyhow::anyhow!("evidence is not shippable: {rejected}"))?;

    let code = match (&config.wasm, &config.contract_store) {
        (Some(wasm), _) => read_file(wasm)?,
        (None, Some(store)) => find_code_for_instance(store, &evidence)?,
        (None, None) => anyhow::bail!(
            "verifying evidence needs the contract it accuses: pass --wasm, or \
             --contract-store pointing at a node that hosts it"
        ),
    };

    let mut oracle = RuntimeOracle::standalone(code, evidence.parameters.clone())
        .await
        .map_err(describe_oracle_build_error)?;

    // The contract must be the one the evidence names. Checking this is what stops
    // a finding being replayed against a different contract and appearing to
    // confirm or clear it.
    if oracle.instance_id() != evidence.contract {
        anyhow::bail!(
            "the contract supplied is {} but the evidence is about {}",
            oracle.instance_id(),
            evidence.contract
        );
    }

    let case = evidence.to_case();
    let outcome = verify_case(&mut oracle, &case);

    println!("evidence {}", evidence.id());
    println!("  contract : {}", evidence.contract);
    println!("  property : {}", evidence.property);
    println!(
        "  claimed  : {}",
        match &evidence.observed {
            Some(v) => format!("{} ({})", v.property, v.detail),
            None => "nothing recorded by the sender".to_string(),
        }
    );

    match &outcome {
        PropertyOutcome::Violated(v) => {
            println!(
                "  verdict  : REPRODUCED \u{2014} {} ({})",
                v.property, v.detail
            );
            if evidence
                .observed
                .as_ref()
                .is_some_and(|o| o.property != v.property)
            {
                println!("  note     : a DIFFERENT law broke here than the sender reported");
            }
            if v.severity == Severity::Violation {
                return Err(ConformanceViolations { count: 1 }.into());
            }
            Ok(())
        }
        PropertyOutcome::Holds => {
            println!("  verdict  : NOT REPRODUCED \u{2014} the law holds on this runtime");
            println!(
                "  note     : the sender's runtime was {:?}; a finding that does not \
                 reproduce must not be acted on",
                evidence.runtime
            );
            Ok(())
        }
        PropertyOutcome::Inconclusive(reason) => {
            println!("  verdict  : INCONCLUSIVE \u{2014} {reason}");
            Ok(())
        }
    }
}

/// Find the code an evidence file's contract instance was built from.
///
/// Derives each candidate's instance id from (its code, the evidence's parameters)
/// and keeps the one that matches. That is an identity check rather than a name
/// lookup: unlike a bundle, evidence carries no code hash to address the store
/// with, and the same code under different parameters is a different contract whose
/// findings must never be mixed up with this one's.
fn find_code_for_instance(store: &Path, evidence: &ConformanceEvidence) -> anyhow::Result<Vec<u8>> {
    let params = freenet_stdlib::prelude::Parameters::from(evidence.parameters.clone());
    let mut examined = 0usize;

    for entry in std::fs::read_dir(store)
        .with_context(|| format!("reading contract store {}", store.display()))?
    {
        let path = entry
            .with_context(|| format!("listing contract store {}", store.display()))?
            .path();
        if !path.is_file() || path.extension().is_none_or(|ext| ext != "wasm") {
            continue;
        }
        // Same versioned encoding as any store entry — see `find_code_in_store`.
        let Ok((code, _version)) = ContractCode::load_versioned_from_path(&path) else {
            continue;
        };
        examined += 1;
        if ContractInstanceId::from_params_and_code(&params, &code) == evidence.contract {
            return Ok(code.data().to_vec());
        }
    }

    anyhow::bail!(
        "no contract in {} is instance {} ({} candidate(s) examined). A peer only \
         stores contracts it hosts, so pass --wasm if this node does not.",
        store.display(),
        evidence.contract,
        examined
    )
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

        // Print the bundle's own provenance note before replaying it.
        //
        // Not decoration. Capture records in this note when it REFUSED related-contract
        // state, and a corpus missing that state cannot reach a verdict: every case
        // comes back Inconclusive, the command exits 0, and the run reads as a clean
        // bill of health for a contract nothing actually judged. Measured on a live
        // corpus: 9 of 54 contracts produced no verdict on any of 2,474 cases for
        // exactly this reason, with nothing in the replay output saying so. The note is
        // the only durable record - node logs rotate long before a corpus is replayed.
        if let Some(note) = bundle.note.as_deref() {
            // STDERR deliberately. `--json` writes one JSON document to stdout, and a
            // plain line ahead of it corrupts the stream for anything parsing it -
            // `fdev conformance --bundle x --json | jq` would simply fail. `write_bundle`
            // in this same file already uses `eprintln!` for its status line for
            // exactly this reason; the first version of this did not follow it.
            //
            // Still visible in ordinary terminal use, which is the case that matters:
            // a reader replaying a corpus needs to see that it may be incomplete.
            eprintln!("bundle note: {note}");
        }

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
    state_candidates: &[Bytes],
    delta_candidates: &[Bytes],
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
        let (minimized, shrink) = minimize(
            oracle,
            case,
            state_candidates,
            delta_candidates,
            &MinimizeConfig::default(),
        );
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

/// Source pin on `load_inputs`' output stream.
///
/// `--json` promises a machine-readable report, which means stdout must carry ONE JSON
/// document and nothing else. `load_inputs` runs before the report is emitted, so
/// anything it prints to stdout lands ahead of that document and breaks every consumer
/// that parses it — `fdev conformance --bundle x --json | jq` simply fails.
///
/// That shipped once: the bundle note was printed with `println!`, and because every
/// bundle this codebase writes always sets a note, it fired on essentially every
/// `--bundle` replay. It was caught in review rather than by CI, because verifying it
/// by hand (running the binary and piping to `jq`, which is how the fix was confirmed)
/// leaves nothing behind that a later refactor has to keep true.
///
/// A pin rather than an integration test: spawning `fdev` against a real bundle needs a
/// contract store and a WASM runtime, which is a large amount of fixture to guard a
/// one-token property. This asserts the property at the only place it can regress.
#[cfg(test)]
mod stdout_purity_pin {
    /// Slice `load_inputs`' body by counting braces to its own closing one.
    ///
    /// Brace-counting rather than "up to the next `fn`": a region ended on a guessed
    /// anchor silently widens when the following item is not the shape assumed, and a
    /// widened region here would swallow the human-report code — which prints to stdout
    /// legitimately — and pass vacuously.
    fn load_inputs_body() -> &'static str {
        let src = include_str!("conformance.rs");
        let start = src
            .find("fn load_inputs(")
            .expect("load_inputs not found in conformance.rs");
        let after = &src[start..];
        let open = after.find('{').expect("load_inputs has no body");
        let mut depth = 0usize;
        for (offset, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &after[..open + offset + 1];
                    }
                }
                _ => {}
            }
        }
        panic!("load_inputs' body is not brace-balanced");
    }

    /// Strip whole-line comments, so a comment mentioning `println!` cannot satisfy or
    /// defeat the assertion. The comment above the `eprintln!` call names both.
    fn code_only() -> String {
        load_inputs_body()
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// `code_only` with the stderr macros removed.
    ///
    /// Needed because `eprintln!` CONTAINS `println!` as a substring - the first
    /// version of this pin asserted `!body.contains("println!")` and could therefore
    /// never pass while the correct `eprintln!` call was present. It failed on correct
    /// code, which is the harmless direction; the same trap the other way is how a pin
    /// passes vacuously forever.
    fn stdout_macros_only() -> String {
        code_only().replace("eprintln!", "").replace("eprint!", "")
    }

    #[test]
    fn load_inputs_never_writes_to_stdout() {
        let body = stdout_macros_only();
        assert!(
            !body.contains("println!") && !body.contains("print!("),
            "load_inputs writes to stdout, which lands ahead of the --json document \
             and corrupts it for every consumer that parses stdout"
        );
    }

    #[test]
    fn the_bundle_note_still_reaches_the_reader() {
        let body = code_only();
        assert!(
            body.contains("eprintln!"),
            "the bundle note is no longer surfaced at all; a corpus whose related \
             state was refused would replay as a clean bill of health"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet::conformance::Violation;

    /// Evidence is resolved by deriving each candidate's instance id, not by name.
    ///
    /// The property that matters: the SAME code under DIFFERENT parameters is a
    /// different contract, and its findings must never be attributed to this one.
    /// A resolver that stopped at "this store contains the right code" would hand
    /// back a contract that merges differently, and the re-execution would then
    /// confirm or clear a finding about something else entirely.
    #[test]
    fn evidence_resolves_by_instance_so_parameters_cannot_be_confused() {
        let store = tempfile::tempdir().expect("temp dir");
        let code = b"\0asm-stand-in-bytes".to_vec();
        let encoded = ContractCode::from(code.clone())
            .to_bytes_versioned(freenet_stdlib::prelude::APIVersion::Version0_0_1)
            .expect("encode versioned");
        std::fs::write(
            store
                .path()
                .join(CodeHash::from_code(&code).encode())
                .with_extension("wasm"),
            &encoded,
        )
        .expect("write store entry");

        let params = vec![1, 2, 3];
        let instance_for = |p: Vec<u8>| {
            ContractInstanceId::from_params_and_code(
                freenet_stdlib::prelude::Parameters::from(p),
                ContractCode::from(code.clone()),
            )
        };
        let case = ConformanceCase::new(
            ConformanceProperty::StateCommutativity,
            vec![Bytes::from(vec![1u8]), Bytes::from(vec![2u8])],
        );

        let right = ConformanceEvidence::new(instance_for(params.clone()), params, &case, None);
        assert_eq!(
            find_code_for_instance(store.path(), &right).expect("should resolve"),
            code,
            "evidence naming this instance must resolve to its code"
        );

        // Evidence whose claimed instance does not match its OWN parameters must
        // not resolve. The instance binds code and parameters together, so this is
        // the check that stops a forged or corrupted claim being replayed against
        // whatever code happens to be lying around — the store here holds
        // byte-identical WASM, and that must still not be enough.
        //
        // My first version of this test asserted the wrong thing: it varied the
        // parameters and the claimed instance together, which is a perfectly
        // consistent different contract and resolves correctly.
        let wrong =
            ConformanceEvidence::new(instance_for(vec![9, 9, 9]), vec![1, 2, 3], &case, None);
        let err = find_code_for_instance(store.path(), &wrong)
            .expect_err("a different instance must not resolve to this contract");
        assert!(
            err.to_string().contains("is instance"),
            "error should name the instance it could not find, got: {err}"
        );
    }

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

    /// Build a violation for a property whose real severity is `severity`.
    ///
    /// Takes the property rather than forging the severity field: severity is
    /// derived from the property now, so a `StateCommutativity` violation labelled
    /// `Diagnostic` is a pair that cannot occur, and a test built on one was
    /// really testing that the field is trusted.
    fn violation_of(property: ConformanceProperty) -> Violation {
        Violation {
            property,
            severity: property.severity(),
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
        // DeltaIdempotence is diagnostic-only: contested, and never removal-eligible.
        let outcome =
            PropertyOutcome::Violated(violation_of(ConformanceProperty::DeltaIdempotence));
        assert_eq!(exit_code([&outcome]), 0);
    }

    #[test]
    fn exit_code_treats_enforceable_violation_as_failure() {
        let outcome =
            PropertyOutcome::Violated(violation_of(ConformanceProperty::StateCommutativity));
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

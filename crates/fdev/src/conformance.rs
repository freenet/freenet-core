//! `fdev verify-merge`: check a contract's merge laws offline (RFC #5320).
//!
//! A contract that breaks them cannot converge — peers given the same updates
//! in different orders end up with different state and never agree.
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
//! fdev verify-merge --wasm contract.wasm --state s1.bin --state s2.bin
//!
//! # Replay a bundle captured from the network (or an earlier `fdev` run)
//! fdev verify-merge --bundle observed.bin
//!
//! # Supply an observed step by hand: the state held, then the state reached
//! fdev verify-merge --wasm contract.wasm --transition before.bin after.bin
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Context;
use freenet::conformance::ConformanceOracle;
use freenet::conformance::generator::Corpus;
use freenet::conformance::host_clock;
use freenet::conformance::verifier::Bytes;
use freenet::conformance::{
    ConformanceCase, ConformanceEvidence, ConformanceProperty, EVIDENCE_SCHEMA_VERSION,
    EvidenceRejected, GeneratorConfig, Inconclusive, MinimizeConfig, OracleBuildError,
    PropertyOutcome, ReplayBundle, RuntimeOracle, Severity, Transition, generate_cases, minimize,
    verify_case,
};
use freenet_stdlib::prelude::{CodeHash, ContractCode, ContractInstanceId, State, UpdateData};
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

    /// An observed step: the state a peer held, and the state it reached after
    /// applying an update. Takes two paths. Repeat for multiple steps.
    ///
    /// This is PROVENANCE, and `transition_path_agreement` cannot be checked
    /// without it. Loose `--state` files say only that both states existed; a
    /// transition says the second was reached FROM the first, which is what makes
    /// "merging it back must reproduce it" a law rather than an accusation of
    /// last-write-wins against every conforming contract.
    ///
    /// ARGUMENT ORDER IS LOAD-BEARING AND CANNOT BE VERIFIED. BASE is the state
    /// the peer HELD; RESULT is the state it REACHED. Supplied the other way
    /// round, a perfectly conforming contract is reported as violating the law,
    /// and nothing distinguishes that from a real finding — you are the witness
    /// this property rests on. A likely-reversed pair is warned about, but the
    /// check is a heuristic and cannot be conclusive.
    ///
    /// A capture bundle already carries these (`ReplayBundle::transitions`); this
    /// is how a developer supplies one by hand from two state files.
    #[arg(long = "transition", num_args = 2, value_names = ["BASE", "RESULT"])]
    pub(crate) transitions: Vec<PathBuf>,

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
    let LoadedInputs {
        wasm,
        parameters,
        corpus,
        hand_supplied_steps,
    } = load_inputs(&config)?;

    // Computed as soon as the code is in hand, and BEFORE the two guards below
    // that abort without building a `Report`. "Your contract reads the host
    // clock" is the cheapest answer this command has and it does not depend on
    // having a workable corpus, so gating it behind one meant an author whose
    // corpus was too thin to generate a case — the common state early in
    // development, and exactly when this is most useful — was told nothing.
    // The normal path prints it as part of the report; the guards print it
    // themselves, since they never reach one.
    let code_diagnostics = code_diagnostics(&wasm);

    if corpus.is_empty() {
        report_code_diagnostics_standalone(&code_diagnostics);
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
        report_code_diagnostics_standalone(&code_diagnostics);
        anyhow::bail!(
            "no cases could be generated from this corpus: {} state(s), {} delta(s), \
             {} summary/summaries for the selected properties. Nothing was checked, \
             so this is not a pass — supply more states (commutativity and \
             reconciliation need at least two), captured deltas for the delta \
             properties, or --transition BASE RESULT for transition_path_agreement.",
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
    let _ = warn_on_reversed_transitions(&mut oracle, &hand_supplied_steps);

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

    let report = Report::build(&corpus, &outcomes, evidence, code_diagnostics);
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

/// Warn when a hand-supplied `--transition BASE RESULT` pair looks reversed.
///
/// Argument order is load-bearing here and unverifiable. Every other malformed input
/// to this command produces an error: an unreadable file, a WASM that will not load,
/// a property name that does not exist. A REVERSED pair is the single exception,
/// because it is structurally indistinguishable from a genuine finding - the whole
/// reason `transition_path_agreement` is a law is that the corpus witnesses which
/// state came first, and when a human types the two paths there is no witness but
/// the order they typed them in.
///
/// The tell is that the law holds in the OTHER direction: if `merge(result, base)`
/// reproduces `base`, then `base` sits above `result` in the merge's own order, so
/// whatever was typed as `RESULT` is the earlier state. For a grow-only contract
/// given the pair backwards this is exactly what happens, and the forward check then
/// reports a violation against a perfectly conforming contract.
///
/// A warning rather than an error: a contract that genuinely disagrees in both
/// directions is possible, and refusing to run would make a real finding
/// unreportable. Stderr for the same reason `write_bundle` uses it - `--json` owns
/// stdout.
/// Returns how many pairs looked reversed, so a test can assert on the decision
/// rather than on whether a line reached stderr.
fn warn_on_reversed_transitions<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    steps: &[(Bytes, Bytes)],
) -> usize {
    let mut suspicious = 0;
    for (base, result) in steps {
        let reversed =
            oracle.update_state(result, &[UpdateData::State(State::from(base.to_vec()))]);
        let Ok(modification) = reversed else {
            continue;
        };
        let Some(state) = modification.new_state else {
            continue;
        };
        if state.as_ref() == base.as_ref() {
            suspicious += 1;
            eprintln!(
                "warning: for one --transition pair, merging BASE into RESULT \
                 reproduces BASE, which is what a reversed pair looks like. \
                 --transition takes the state the peer HELD first and the state it \
                 REACHED second; supplied the other way round, a conforming \
                 contract is reported as violating transition_path_agreement and \
                 nothing can tell that apart from a real finding."
            );
        }
    }
    suspicious
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
    bundle.summaries = corpus.summaries.iter().map(|s| s.to_vec()).collect();
    // Carry the steps through, or `--bundle-out` would silently drop the one input
    // `transition_path_agreement` needs and the replayed bundle would report a clean
    // run where the original found something.
    //
    // Each step also reclaims the delta observed against its base. A `Transition` is
    // the ONLY place a bundle can record what a delta was applied to:
    // `ReplayBundle::to_corpus` fills `Corpus::delta_bases` from
    // `transition.delta` + `transition.base_state`, and bundle-level `deltas` are
    // explicitly given no base. Exporting the deltas loose therefore silently drops
    // every pairing, and `delta_permutation_invariance` - which only pairs deltas
    // observed against the SAME base - checks nothing on the re-exported bundle
    // while the original run checked plenty. A replay that quietly covers less than
    // the run it replays is the worst shape this command can have.
    let (with_bases, loose): (Vec<usize>, Vec<usize>) =
        (0..corpus.deltas.len()).partition(|i| corpus.delta_base(*i).is_some());
    let mut unassigned = with_bases;
    // Take one delta recorded against this exact base, removing it from the pool.
    let take_for = |base: &Bytes, unassigned: &mut Vec<usize>| -> Option<usize> {
        unassigned
            .iter()
            .position(|i| corpus.delta_base(*i).map(|b| b.as_ref()) == Some(base.as_ref()))
            .map(|slot| unassigned.remove(slot))
    };
    let mut steps: Vec<Transition> = Vec::with_capacity(corpus.transitions.len());
    for (base, result) in &corpus.transitions {
        let step = |delta: Option<usize>| Transition {
            base_state: base.to_vec(),
            result_state: result.to_vec(),
            delta: delta.map(|i| corpus.deltas[i].to_vec()),
            ..Default::default()
        };
        steps.push(step(take_for(base, &mut unassigned)));
        // A `Transition` holds at most ONE delta, and a base can legitimately own
        // several. `Corpus::deduplicated` collapses steps by `(base, result)`, so a
        // contract that took two different updates from the same state and landed on
        // the same result arrives here as one step and two same-base deltas — the
        // exact shape `delta_permutation_invariance` is FOR. Stopping at one delta
        // per step therefore sent the second out loose, where `to_corpus` gives it no
        // base and it is never paired: the re-exported bundle checks the property on
        // nothing while the original run checked the pair.
        //
        // So repeat the step, once per further delta against the same base. The
        // duplicate `(base, result)` pairs cost their two states in the encoding but
        // collapse again in `to_corpus`'s own `deduplicated`, leaving one step and
        // every delta still holding its base.
        while let Some(extra) = take_for(base, &mut unassigned) {
            steps.push(step(Some(extra)));
        }
    }
    bundle.transitions = steps;
    // Whatever no step claimed still travels, just without provenance - which is
    // exactly what it had. `unassigned` can still hold deltas here: a corpus may
    // record a base for a delta without holding any step that starts from it.
    bundle.deltas = loose
        .into_iter()
        .chain(unassigned)
        .map(|i| corpus.deltas[i].to_vec())
        .collect();
    bundle.note = Some(format!(
        "captured by fdev verify-merge {}",
        env!("CARGO_PKG_VERSION")
    ));
    bundle
        .write_to(path)
        .with_context(|| format!("writing bundle to {}", path.display()))?;
    eprintln!(
        "wrote replay bundle to {} ({} state(s), {} delta(s), {} summary/summaries, \
         {} transition(s))",
        path.display(),
        bundle.states.len(),
        bundle.deltas.len(),
        bundle.summaries.len(),
        bundle.transitions.len(),
    );
    Ok(())
}

/// The contract broke a merge law. Distinct from any harness failure, so a caller
/// can tell "this contract is unsound" apart from "the check could not run" — a
/// distinction CI needs and a single exit code cannot express.
#[derive(Debug, thiserror::Error)]
#[error(
    "{count} merge-law violation(s) found: this contract cannot converge, so peers \
     holding it will disagree and keep retrying"
)]
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
    // never accepts evidence the network itself would reject, and do it BEFORE
    // building the runtime — the point of the check is that nothing unbounded or
    // unsound reaches the WASM. `to_case` re-runs it below; that is deliberate
    // belt-and-braces, since `to_case` is the enforcement point and this call is the
    // one that produces a good error message early.
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

    // Same reasoning as in `conformance()`: this is a fact about the code, it is
    // free, and this path has the code in hand. It is reported BEFORE the
    // instance-id check below, because a contract that reads the clock is worth
    // saying so about even when it turns out not to be the one the evidence
    // accuses.
    report_code_diagnostics_standalone(&code_diagnostics(&code));

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

    let case = evidence
        .to_case()
        .map_err(|rejected| anyhow::anyhow!("evidence is not shippable: {rejected}"))?;
    let outcome = verify_case(&mut oracle, &case);

    println!("evidence {}", evidence.id());
    println!("  contract : {}", evidence.contract);
    println!("  property : {}", evidence.property);
    // `v.detail` is deserialized from an evidence file ANOTHER PEER produced, and
    // this function's own stated contract is that `evidence.observed` is never
    // trusted. It was nonetheless interpolated raw into the operator's terminal, so
    // a sender could embed a newline and forge a `verdict  :` line in the exact
    // format the real one uses - on the one command here built to consume hostile
    // input. `v.property` is an enum and carries no free text.
    println!(
        "  claimed  : {}",
        match &evidence.observed {
            Some(v) => format!("{} ({})", v.property, present_detail(&v.detail)),
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
            // `Display for Inconclusive` interpolates the contract's own error text
            // raw, so this is the same untrusted-text-to-terminal path the report
            // side escapes. Escaping the rendered line whole is safe: the static
            // prefixes it adds contain nothing escapable.
            println!(
                "  verdict  : INCONCLUSIVE \u{2014} {}",
                present_detail(&reason.to_string())
            );
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
fn load_inputs(config: &ConformanceConfig) -> anyhow::Result<LoadedInputs> {
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
            // `fdev verify-merge --bundle x --json | jq` would simply fail. `write_bundle`
            // in this same file already uses `eprintln!` for its status line for
            // exactly this reason; the first version of this did not follow it.
            //
            // Still visible in ordinary terminal use, which is the case that matters:
            // a reader replaying a corpus needs to see that it may be incomplete.
            eprintln!("bundle note: {note}");
        }

        let corpus = bundle.to_corpus();
        // A bundle's steps carry real provenance from the capture path, so they are
        // not hand-supplied and are not order-checked below.
        Ok(LoadedInputs {
            wasm,
            parameters,
            corpus,
            hand_supplied_steps: Vec::new(),
        })
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
        if config.states.is_empty() && config.transitions.is_empty() {
            // `fdev verify-merge --wasm mycontract.wasm` — no corpus — is the
            // FIRST thing an author runs, and this bail is the third and
            // earliest of the three that abort before a `Report` exists. The
            // other two are guarded in `conformance()`, but this one short
            // circuits `load_inputs(&config)?` so that guard is never reached,
            // which left the single most likely early-development invocation
            // saying nothing about the clock. The WASM is already read above,
            // so the answer costs nothing here.
            //
            // To stderr, so `load_inputs_never_writes_to_stdout` still holds.
            report_code_diagnostics_standalone(&code_diagnostics(&wasm));
            anyhow::bail!(
                "at least one --state or --transition is required unless --bundle is given"
            );
        }
        let mut states = Vec::with_capacity(config.states.len());
        for path in &config.states {
            states.push(read_file(path)?);
        }
        // `num_args = 2` guarantees an even count, so the chunks are always whole
        // pairs; this states that rather than leaving a lone path to be silently
        // dropped if the arg definition is ever loosened. An error rather than a
        // panic: this is a CLI invariant, and a user who somehow reaches it deserves
        // a message rather than a backtrace.
        if config.transitions.len() % 2 != 0 {
            anyhow::bail!(
                "--transition takes two paths per occurrence, got {}",
                config.transitions.len()
            );
        }
        let mut steps: Vec<(Bytes, Bytes)> = Vec::with_capacity(config.transitions.len() / 2);
        for pair in config.transitions.chunks(2) {
            let base = read_file(&pair[0])?;
            let result = read_file(&pair[1])?;
            // Both endpoints are ordinary observed states too, so the other
            // properties get to use them rather than the transition being a
            // dead-end input.
            states.push(base.clone());
            states.push(result.clone());
            steps.push((Bytes::from(base), Bytes::from(result)));
        }
        let corpus = Corpus {
            transitions: steps.clone(),
            ..Corpus::from_states(states)
        }
        .deduplicated();
        Ok(LoadedInputs {
            wasm,
            parameters,
            corpus,
            hand_supplied_steps: steps,
        })
    }
}

/// What a run was given: the contract, its parameters, the corpus, and separately
/// the steps a human typed.
///
/// The last field exists only so [`warn_on_reversed_transitions`] can tell a
/// hand-supplied pair from one a capture recorded. A bundle's steps are witnessed by
/// the node that observed them; a `--transition BASE RESULT` pair is witnessed by
/// nothing but argument order.
#[derive(Debug)]
struct LoadedInputs {
    wasm: Vec<u8>,
    parameters: Vec<u8>,
    corpus: Corpus,
    hand_supplied_steps: Vec<(Bytes, Bytes)>,
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
///
/// Generic over the oracle purely so this function is testable: `minimize` and
/// `verify_case` already are, and pinning this one to `RuntimeOracle` would have made
/// "did minimisation run?" answerable only by scraping the source. A fake oracle that
/// counts its calls answers it directly.
fn write_evidence<O: ConformanceOracle + ?Sized>(
    dir: &PathBuf,
    instance: ContractInstanceId,
    parameters: &[u8],
    outcomes: &[(ConformanceCase, PropertyOutcome)],
    oracle: &mut O,
    state_candidates: &[Bytes],
    delta_candidates: &[Bytes],
) -> anyhow::Result<EvidenceSummary> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating evidence directory {}", dir.display()))?;

    let mut written = HashSet::new();
    let mut oversized = 0usize;
    let mut local_only = 0usize;
    let mut shrunk_from = 0usize;
    let mut shrunk_to = 0usize;
    // Grouped, not per-case. Both of these fire once per VIOLATED CASE, and a corpus
    // routinely produces dozens of cases breaking the same law — `group_findings`
    // exists for exactly that reason on the report side. Printing one identical line
    // per case buries the report the note is telling the reader to go and look at.
    let mut local_only_notes: Vec<UnwritableNote> = Vec::new();
    let mut oversized_notes: Vec<UnwritableNote> = Vec::new();
    for (case, outcome) in outcomes {
        if !outcome.is_enforceable_violation() {
            continue;
        }

        // A property whose premise the bytes cannot carry is not evidence at all,
        // and saying "could not be reduced to a shippable size" about one would be a
        // false explanation of a permanent condition. It is not a failure either:
        // the finding is real and this command reported it, it simply never travels.
        //
        // Checked BEFORE minimising, not after. Minimisation is the expensive part of
        // this loop — repeated `verify_case` calls through the WASM runtime — and
        // spending all of it on a case that is then discarded is pure waste. It also
        // keeps the byte counters honest: they are documented as summed over the
        // findings written, and accumulating for a finding shrinking could never help
        // puts a number in the denominator that the ratio is not about.
        if !case.property.is_self_verifying() {
            local_only += 1;
            note_unwritable(&mut local_only_notes, case.property, None);
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
            note_unwritable(
                &mut oversized_notes,
                minimized.property,
                Some(rejected.to_string()),
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

    // Stderr, never stdout: `--json` promises one JSON document on stdout and these
    // notes would corrupt it. See the `stdout_purity_pin` module.
    let _ = write_unwritable_notes(
        &mut std::io::stderr().lock(),
        &local_only_notes,
        &oversized_notes,
    );

    Ok(EvidenceSummary {
        directory: dir.display().to_string(),
        files_written: written.len(),
        findings_local_only: local_only,
        findings_too_large: oversized,
        input_bytes_before_shrinking: shrunk_from,
        input_bytes_after_shrinking: shrunk_to,
    })
}

/// One "no evidence file was written" note, accumulated per property rather than
/// emitted per case.
struct UnwritableNote {
    property: ConformanceProperty,
    cases: usize,
    /// A representative rejection, where the reason varies per case (the byte counts
    /// in an over-size rejection do). The count is what the reader acts on; the
    /// example is what tells them which knob it is about.
    example: Option<String>,
}

/// Bump this property's note, keeping the FIRST example seen.
///
/// First rather than last so the line is stable across a re-run that produces the
/// same findings in the same order — a note whose numbers move between identical runs
/// reads as new information.
fn note_unwritable(
    notes: &mut Vec<UnwritableNote>,
    property: ConformanceProperty,
    example: Option<String>,
) {
    match notes.iter_mut().find(|n| n.property == property) {
        Some(note) => note.cases += 1,
        None => notes.push(UnwritableNote {
            property,
            cases: 1,
            example,
        }),
    }
}

/// Emit the grouped notes, one line per property rather than one per case.
///
/// Split out from `write_evidence` so the grouping can be asserted: `write_evidence`
/// needs a WASM oracle and a temp directory, and its notes go to stderr, which a test
/// cannot capture.
fn write_unwritable_notes(
    out: &mut impl std::io::Write,
    local_only: &[UnwritableNote],
    oversized: &[UnwritableNote],
) -> std::io::Result<()> {
    for note in local_only {
        writeln!(
            out,
            "note: {} {} finding(s) cannot be carried as evidence at all ({}); they \
             appear in the report's findings list and no evidence file is written \
             for them",
            note.cases,
            note.property,
            EvidenceRejected::NotSelfVerifying {
                property: note.property
            }
        )?;
    }
    for note in oversized {
        writeln!(
            out,
            "warning: {} {} finding(s) stayed over the evidence size limit even after \
             shrinking, so no evidence file was written for them — example: {}",
            note.cases,
            note.property,
            note.example.as_deref().unwrap_or("(no detail)")
        )?;
    }
    Ok(())
}

#[derive(Serialize)]
struct EvidenceSummary {
    directory: String,
    files_written: usize,
    /// Findings from a property that is not self-verifying, so no evidence can be
    /// built for it at all. Reported rather than swallowed, and deliberately NOT
    /// counted as "too large": the reason is permanent and has nothing to do with
    /// size. See `ConformanceProperty::premise_source`.
    findings_local_only: usize,
    /// Findings that stayed over the evidence size bound even after shrinking.
    /// Reported rather than swallowed: they are real findings that simply cannot be
    /// propagated, and silently writing nothing would look like there were none.
    findings_too_large: usize,
    /// Total case input bytes before and after minimisation, summed over every
    /// finding minimisation was actually RUN on — which is not the same set as the
    /// files written, and the difference is deliberate in both directions.
    ///
    /// A finding counted by `findings_too_large` is included: shrinking ran, and
    /// failed to get it under the bound. Excluding it would drop exactly the cases
    /// where shrinking did least and overstate the ratio.
    ///
    /// A finding counted by `findings_local_only` is excluded, because minimisation
    /// never runs for one: the property cannot travel as evidence whatever its size,
    /// so shrinking it could not have helped and its bytes are not a measure of
    /// anything. That exclusion is structural — the check sits above the `minimize`
    /// call — rather than a subtraction someone has to remember.
    ///
    /// Duplicates are included too: a second case with the same evidence id had
    /// minimisation run on it before the id was known.
    ///
    /// So the ratio is the honest measure of whether shrinking is earning its keep on
    /// the work it is actually asked to do.
    input_bytes_before_shrinking: usize,
    input_bytes_after_shrinking: usize,
}

/// A finding about the contract's CODE rather than about the outcome of checking
/// a merge law.
///
/// [`Finding`] cannot express one. Its `property`, `left` and `right` fields are
/// the law that was checked and the digests of the two states that disagreed, and
/// there is no honest value for any of them here: nothing was executed, no state
/// was produced, and no law was violated — the contract simply contains something
/// worth telling its author about. Forcing it into a `Finding` would mean inventing
/// a property name that `--property` cannot select and `ConformanceProperty::ALL`
/// does not list, and stamping two empty digests on it. So this is a separate,
/// deliberately small channel alongside the findings, and it is
/// **never removal-eligible**: [`exit_code`] reads `PropertyOutcome`s only, so a
/// code diagnostic cannot fail the command however it is worded.
#[derive(Serialize, Clone, PartialEq, Eq, Debug)]
struct CodeDiagnostic {
    /// Stable machine-readable name, for a `--json` consumer.
    diagnostic: &'static str,
    /// What the author needs to know, in one paragraph.
    detail: String,
}

/// Code-level diagnostics for the contract under check.
///
/// Kept as its own function, taking bytes and returning values, so the DECISION
/// is testable without a WASM runtime, a corpus, or a running `conformance()`.
fn code_diagnostics(wasm: &[u8]) -> Vec<CodeDiagnostic> {
    let mut diagnostics = Vec::new();
    if host_clock::imports_host_clock(wasm) {
        diagnostics.push(CodeDiagnostic {
            diagnostic: "host_clock_import",
            detail: format!(
                "this contract imports the host wall clock ({}::{}), which is \
                 DEPRECATED for contracts. update_state must be a function of its \
                 inputs or replicas cannot be guaranteed to converge, so the merge \
                 laws checked above are not well-formed statements about a contract \
                 that reads the clock. In a future release the call will TRAP \
                 (issue #5465): the contract will still load, but any actual call \
                 to the clock will fail that operation. Trapping is per-call, so a \
                 contract that imports the symbol without reaching it keeps working \
                 and needs no re-key. Carry a client-signed timestamp in the state \
                 and enforce only monotonicity (new > current) instead. Delegates \
                 are unaffected. See {}",
                host_clock::HOST_CLOCK_NAMESPACE,
                host_clock::HOST_CLOCK_IMPORT,
                host_clock::HOST_CLOCK_DEPRECATION_DOC,
            ),
        });
    }
    diagnostics
}

/// The standalone rendering of code diagnostics, for the paths that never build
/// a [`Report`], or `None` when there is nothing to say.
///
/// Pure, so the decision is testable without running the command.
fn render_code_diagnostics_standalone(diagnostics: &[CodeDiagnostic]) -> Option<String> {
    if diagnostics.is_empty() {
        return None;
    }
    let mut out =
        String::from("code diagnostics (about the contract's code, not about a law it broke):\n");
    for d in diagnostics {
        out.push_str(&format!("  - {}: {}\n", d.diagnostic, d.detail));
    }
    Some(out)
}

/// Print code diagnostics on a path that aborts before a [`Report`] exists.
///
/// To STDERR deliberately. These call sites either precede an `anyhow::bail!`
/// (so the run is failing and stdout may be a `--json` document a consumer is
/// parsing) or sit in `verify_evidence`, whose stdout is its own fixed
/// `key : value` report. In neither case may this be allowed to appear in the
/// middle of stdout.
///
/// Note the node-side WARN does not cover these paths either:
/// `conformance_log_level` returns `LevelFilter::OFF` for any `--json` run, so
/// without this the answer exists nowhere.
fn report_code_diagnostics_standalone(diagnostics: &[CodeDiagnostic]) {
    if let Some(text) = render_code_diagnostics_standalone(diagnostics) {
        eprint!("{text}");
    }
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
    /// See [`CodeDiagnostic`]. Separate from `findings` because these are not
    /// property outcomes and must never be counted as violations.
    code_diagnostics: Vec<CodeDiagnostic>,
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

/// How many distinct detail texts the HUMAN report shows per inconclusive reason.
///
/// The texts come from contract code, so their number is bounded only by the case
/// count: a contract that embeds a state hash in its error message produces a
/// distinct string per case, and listing 435 of those would bury the question the
/// reader actually has ("is this one bug, or eight?") rather than answer it. Five
/// separates those two cases comfortably.
///
/// This is a READABILITY bound and so it applies to the human report alone. `--json`
/// carries every distinct message: a machine consumer has no width constraint, the
/// tally already holds them all so nothing is saved by dropping them, and a capped
/// machine format would hand a hostile contract a cheap way to bury its real failure
/// behind five high-frequency decoys. The human report says how much it is not
/// showing, in both messages and cases, and points at `--json` for the rest.
const MAX_INCONCLUSIVE_EXAMPLES: usize = 5;

/// Longest detail text reported, in characters after escaping.
///
/// Contract-authored strings have no length bound and this text is copied into bug
/// reports, so it is capped. Deduplication happens on the presented (escaped and
/// truncated) form, so two messages sharing a long prefix count as one — which is
/// why the overflow count below says "distinct message(s)" rather than "distinct
/// error(s)".
const MAX_INCONCLUSIVE_EXAMPLE_CHARS: usize = 200;

#[derive(Serialize)]
struct InconclusiveReason {
    reason: &'static str,
    occurrences: usize,
    /// Every distinct text behind this reason, most frequent first. Empty for the
    /// reasons that carry no text at all (`RoundLimit`, `RelatedRequired`, ...).
    ///
    /// Complete here on purpose; only the human report trims it. See
    /// [`MAX_INCONCLUSIVE_EXAMPLES`].
    examples: Vec<InconclusiveExample>,
}

/// One distinct detail text and how many cases produced it.
///
/// The text is written by the CONTRACT, so it can carry real application state - a
/// rejected value, a member key, a room id - and the corpora these runs replay are
/// captured from live peers. `bundle.rs` and `capture.rs` both mark that material
/// sensitive; this is the path that lifts it out of those files and onto a terminal,
/// from which it is easily pasted into a bug report. The human report says so where
/// it prints them; treat `--json` containing them the same way.
#[derive(Serialize)]
struct InconclusiveExample {
    text: String,
    occurrences: usize,
}

/// "1 case" / "N cases". The findings section already reads this way; the
/// inconclusive examples printed "1 case(s)".
fn case_count(n: usize) -> String {
    if n == 1 {
        "1 case".to_string()
    } else {
        format!("{n} cases")
    }
}

/// Running tally for one inconclusive reason while a report is being built.
#[derive(Default)]
struct ReasonTally {
    occurrences: usize,
    /// `BTreeMap` so equally-frequent texts order deterministically: two runs over
    /// the same corpus must produce byte-identical reports, or diffing them (the
    /// normal way to see whether a contract change helped) is worthless.
    details: BTreeMap<String, usize>,
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
        code_diagnostics: Vec<CodeDiagnostic>,
    ) -> Self {
        let mut holds = 0usize;
        let mut violations = 0usize;
        let mut enforceable_violations = 0usize;
        let mut diagnostic_violations = 0usize;
        let mut inconclusive = 0usize;

        let mut findings: Vec<Finding> = Vec::new();
        let mut inconclusive_reasons: HashMap<&'static str, ReasonTally> = HashMap::new();

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
                    let tally = inconclusive_reasons
                        .entry(inconclusive_label(reason))
                        .or_default();
                    tally.occurrences += 1;
                    if let Some(detail) = inconclusive_detail(reason) {
                        *tally.details.entry(present_detail(detail)).or_insert(0) += 1;
                    }
                }
            }
        }

        let mut inconclusive_reasons: Vec<InconclusiveReason> = inconclusive_reasons
            .into_iter()
            .map(|(reason, tally)| {
                let mut details: Vec<(String, usize)> = tally.details.into_iter().collect();
                // Most frequent first, then lexicographic. The frequency order makes
                // the first line the representative case; the lexicographic tiebreak
                // is what keeps the choice from depending on hash iteration order.
                details.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                InconclusiveReason {
                    reason,
                    occurrences: tally.occurrences,
                    examples: details
                        .into_iter()
                        .map(|(text, occurrences)| InconclusiveExample { text, occurrences })
                        .collect(),
                }
            })
            .collect();
        // Ties broken by label for the same reason as above: a report that reorders
        // between identical runs cannot be diffed.
        inconclusive_reasons.sort_by(|a, b| {
            b.occurrences
                .cmp(&a.occurrences)
                .then_with(|| a.reason.cmp(b.reason))
        });

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
            code_diagnostics,
        }
    }

    /// Print the human report to stdout.
    ///
    /// A thin wrapper over [`Report::write_human`] so the report's TEXT can be
    /// rendered into a buffer and asserted on. The pin that used to guard the
    /// no-evidence explanations scraped this function's source for two field
    /// names, and `fn_body` returns raw source INCLUDING comments — so the
    /// comment above those two blocks satisfied the pin on its own, and deleting
    /// both `println!` blocks left the test green. Rendering to a sink lets the
    /// test assert the lines actually appear.
    ///
    /// Write errors are dropped: a closed stdout (`| head`) is not a run failure,
    /// and there is nowhere left to report it to anyway.
    fn print_human(&self) {
        let _ = self.write_human(&mut std::io::stdout().lock());
    }

    fn write_human(&self, out: &mut impl std::io::Write) -> std::io::Result<()> {
        writeln!(
            out,
            "merge check: {} state(s), {} delta(s), {} summary/summaries in the corpus",
            self.corpus_states, self.corpus_deltas, self.corpus_summaries
        )?;
        writeln!(
            out,
            "merge check: {} case(s) run \u{2014} {} held, {} violation(s) ({} enforceable, {} diagnostic-only), {} inconclusive",
            self.cases_run,
            self.holds,
            self.violations,
            self.enforceable_violations,
            self.diagnostic_violations,
            self.inconclusive
        )?;

        if !self.findings.is_empty() {
            writeln!(out, "\nfindings:")?;
            for (f, count) in group_findings(&self.findings) {
                let cases = if count == 1 {
                    "1 case".to_string()
                } else {
                    format!("{count} cases")
                };
                writeln!(
                    out,
                    "  [{}] {} ({cases}): {}\n      example \u{2014} left: {}; right: {}",
                    f.severity, f.property, f.detail, f.left, f.right
                )?;
            }
        }

        // Inconclusive results are not failures. A contract that legitimately
        // rejects updates or requires related state produces many of these,
        // and printing them as errors would train an author to "fix" working
        // code.
        if !self.inconclusive_reasons.is_empty() {
            writeln!(
                out,
                "\ninconclusive ({} total \u{2014} NOT passes: these cases reached no verdict, so they say nothing about the contract):",
                self.inconclusive
            )?;
            // Said once, above the messages rather than beside each one: these
            // strings come from the contract, and a contract error routinely names
            // what it rejected. The corpora replayed here are captured from live
            // peers, and `bundle.rs` and `capture.rs` both mark that material
            // sensitive - this is the path that brings it to a terminal.
            if self
                .inconclusive_reasons
                .iter()
                .any(|r| !r.examples.is_empty())
            {
                writeln!(
                    out,
                    "  (quoted messages are written by the contract and may contain \
                     application state)"
                )?;
            }
            for r in &self.inconclusive_reasons {
                writeln!(out, "  {}: {}", r.reason, r.occurrences)?;
                // The text the contract actually produced. Without it the largest
                // text-carrying bucket is a number and nothing else, and a reader
                // cannot tell one recurring bug from many unrelated ones.
                //
                // Quoted because `escape_debug` escapes `"`, so the quotes mark the
                // exact extent of contract-authored text. Without them a message of
                // wide or combining characters can wrap past the terminal width and
                // its tail reads like a line of the report.
                for example in r.examples.iter().take(MAX_INCONCLUSIVE_EXAMPLES) {
                    writeln!(
                        out,
                        "      {} \u{2014} \"{}\"",
                        case_count(example.occurrences),
                        example.text
                    )?;
                }
                let hidden = r.examples.len().saturating_sub(MAX_INCONCLUSIVE_EXAMPLES);
                if hidden > 0 {
                    // Both numbers, because "3 further messages" alone cannot
                    // distinguish 3 stray cases from 300, and the reader needs to
                    // know whether what is hidden is the bulk of the bucket.
                    let hidden_cases: usize = r
                        .examples
                        .iter()
                        .skip(MAX_INCONCLUSIVE_EXAMPLES)
                        .map(|e| e.occurrences)
                        .sum();
                    writeln!(
                        out,
                        "      \u{2026} and {hidden} further distinct message(s), \
                         {hidden_cases} case(s), not shown \u{2014} --json has all of them"
                    )?;
                }
            }
        }

        if let Some(evidence) = &self.evidence {
            writeln!(
                out,
                "\nwrote {} evidence file(s) to {}",
                evidence.files_written, evidence.directory
            )?;
            // Never let that line stand alone when it says zero.
            //
            // `findings_local_only` and `findings_too_large` are the only two ways a
            // real finding produces no file, and the default human output printed
            // neither — so "wrote 0 evidence file(s)" read exactly like a clean run,
            // which is the failure mode `findings_too_large`'s own doc comment warns
            // about ("silently writing nothing would look like there were none").
            // Only `--json` carried the reason, and the person who most needs it is
            // the one who did not ask for JSON.
            if evidence.findings_local_only > 0 {
                writeln!(
                    out,
                    "  {} finding(s) came from a property whose premise the evidence \
                     bytes cannot carry, so no evidence exists to write; they are \
                     listed above and are local-only by design",
                    evidence.findings_local_only
                )?;
            }
            if evidence.findings_too_large > 0 {
                writeln!(
                    out,
                    "  {} finding(s) stayed over the evidence size limit even after \
                     shrinking; they are listed above and simply cannot be \
                     propagated",
                    evidence.findings_too_large
                )?;
            }
        }

        if !self.code_diagnostics.is_empty() {
            writeln!(
                out,
                "\ncode diagnostics (about the contract's code, not about a law it broke; these never fail this command):"
            )?;
            for d in &self.code_diagnostics {
                writeln!(out, "  [{}] {}", d.diagnostic, d.detail)?;
            }
        }

        if self.enforceable_violations == 0 {
            writeln!(out, "\nno enforceable violations found.")?;
            if self.diagnostic_violations > 0 {
                writeln!(
                    out,
                    "({} diagnostic finding(s) above are efficiency notes, not merge-law breaks, and do not fail this command.)",
                    self.diagnostic_violations
                )?;
            }
            // Without this the line above is the last thing a reader sees, and
            // "no enforceable violations found." reads as a clean bill of health
            // for a contract whose clock call will trap in a future release.
            if !self.code_diagnostics.is_empty() {
                writeln!(
                    out,
                    "({} code diagnostic(s) above are about the contract's code rather than a merge law, so they do not fail this command, but they still need addressing.)",
                    self.code_diagnostics.len()
                )?;
            }
        }

        Ok(())
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
        Inconclusive::NoDeltaPath => "no delta path to compare against",
        Inconclusive::StateNotSettled => "observed result state never settles",
        Inconclusive::NotReproducible => "finding did not reproduce",
        _ => "other",
    }
}

/// The text an [`Inconclusive`] carries, where its variant carries one.
///
/// [`inconclusive_label`] deliberately collapses to a fixed set of buckets so the
/// summary stays countable. This is the other half of the same answer, and without
/// it the buckets that DO carry text are undiagnosable from the tool's own output:
/// measured on a 64-bundle live corpus, 435 of 2,622 inconclusive cases were
/// `contract error` and not one of them could be read (#5461).
///
/// The wildcard arm is forced - `Inconclusive` is `#[non_exhaustive]` - and it is
/// exactly how the text was lost in the first place, so a source pin
/// (`inconclusive_detail_covers_every_text_carrying_variant`) fails the build if a
/// new text-carrying variant is added without being listed here.
fn inconclusive_detail(reason: &Inconclusive) -> Option<&str> {
    match reason {
        Inconclusive::ContractError(text)
        | Inconclusive::ResourceLimit(text)
        | Inconclusive::MalformedCase(text) => Some(text),
        _ => None,
    }
}

/// Make a contract-authored string safe and bounded for the report.
///
/// Two hazards, both from one fact: this text is written by the contract under test,
/// which is the thing we are least entitled to trust.
///
/// Control characters are escaped first. A message containing a newline would
/// otherwise forge report lines - a fabricated `violation:` line is one `\n` away -
/// and a bare carriage return would overwrite the line the reader just saw. Escaping
/// before truncating also means the cap bounds what is actually printed rather than
/// what was parsed.
fn present_detail(text: &str) -> String {
    // `str::escape_debug`, not `char::escape_debug` in a loop: the former escapes a
    // LEADING grapheme-extend character and leaves one mid-string alone, and driving
    // it per character would silently lose that distinction.
    //
    // Bounded by [`take_presented`] BEFORE anything is collected, because the
    // iterator is lazy: only as much of the message is read as the cap needs.
    // Escaping the whole string and truncating afterwards bounds the OUTPUT while
    // leaving the WORK unbounded - escaping expands a control character roughly
    // sixfold, this runs once per inconclusive CASE (deduplication happens on the
    // result, not before it), and the contract chooses both length and content. That
    // is a wall-clock denial of service against the tool, not merely an allocation
    // spike.
    take_presented(text.escape_debug())
}

/// The bounded half of [`present_detail`], taken over an iterator rather than a
/// `&str` so that the laziness above is testable: a test can hand this a source that
/// panics the moment it is read past the cap, which is the only way to assert that
/// the work is bounded rather than merely the output.
fn take_presented(escaped: impl Iterator<Item = char>) -> String {
    // One char past the cap is enough to know whether anything was dropped, and is
    // all that is ever read.
    let mut chars: Vec<char> = escaped.take(MAX_INCONCLUSIVE_EXAMPLE_CHARS + 1).collect();
    if chars.len() > MAX_INCONCLUSIVE_EXAMPLE_CHARS {
        chars.truncate(MAX_INCONCLUSIVE_EXAMPLE_CHARS);
        chars.push('\u{2026}');
    }
    chars.into_iter().collect()
}

/// Source pin on `load_inputs`' output stream.
///
/// `--json` promises a machine-readable report, which means stdout must carry ONE JSON
/// document and nothing else. `load_inputs` runs before the report is emitted, so
/// anything it prints to stdout lands ahead of that document and breaks every consumer
/// that parses it — `fdev verify-merge --bundle x --json | jq` simply fails.
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
    /// Slice a function's body by counting braces to its own closing one.
    ///
    /// Brace-counting rather than "up to the next `fn`": a region ended on a guessed
    /// anchor silently widens when the following item is not the shape assumed, and a
    /// widened region here would swallow the human-report code — which prints to stdout
    /// legitimately — and pass vacuously.
    ///
    /// Private on purpose, and reached only through [`code_only`]: the raw body it
    /// returns INCLUDES comments, so a scrape built directly on it is satisfied by a
    /// comment naming the token it searches for. That is not hypothetical — the pin
    /// on the report's no-evidence explanations was written against `fn_body`, and a
    /// comment added by the same commit kept it green after both `println!` blocks it
    /// guarded were deleted. Every scrape in this file now goes through `code_only`,
    /// so the footgun cannot be picked up by reaching for the obvious helper.
    fn fn_body(signature: &str) -> &'static str {
        let src = include_str!("conformance.rs");
        let start = src
            .find(signature)
            .unwrap_or_else(|| panic!("{signature} not found in conformance.rs"));
        // A signature that only matches inside a test module means the real function
        // was renamed or removed: the region would then be some test's body, where the
        // searched-for token is as likely to appear as not. Fail loudly instead.
        if let Some(tests) = src.find("\n#[cfg(test)]") {
            assert!(
                start < tests,
                "{signature} matched only inside a test module, so this pin would be \
                 scoped to a test rather than to production code"
            );
        }
        let after = &src[start..];
        let open = after.find('{').expect("signature has no body");
        // Count braces over a copy with literals and comments blanked out, then
        // slice the ORIGINAL at the offset that finds. Counting over raw source
        // treats a brace inside a string literal as structure, so a
        // `format!("...{...")` added to a scraped function later would silently
        // widen its region into the next function — and both the `count() == 1`
        // assertion and its vacuity anchor would still pass, so the pin would
        // weaken quietly instead of failing. That is the exact failure mode
        // these pins exist to prevent. `blank_literals` panics on syntax it
        // cannot mask, so the failure direction is loud.
        let masked = blank_literals(&after[open..]);
        let mut depth = 0usize;
        for (offset, ch) in masked.char_indices() {
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
        panic!("{signature}'s body is not brace-balanced");
    }

    /// `src` with the CONTENTS of string literals, char literals and comments
    /// replaced by spaces, so brace counting sees structure only. Byte offsets
    /// are preserved exactly, so an offset found in the result indexes the
    /// original.
    ///
    /// Panics on raw strings and block comments rather than guessing at them:
    /// a masker that silently mishandles syntax it does not know is the same
    /// defect as not masking at all. If this fires, extend it — do not delete
    /// the call. (Kept in step with the twin in
    /// `freenet::wasm_runtime::runtime`'s call-site pin; they cannot be shared
    /// across the crate boundary without making test-only helpers public.)
    fn blank_literals(src: &str) -> String {
        // Kept BYTE-IDENTICAL with its twin; see the divergence pin in `fdev`'s
        // `stdout_purity_pin::the_two_blank_literals_have_not_drifted`.
        fn excerpt(src: &str, at: usize) -> &str {
            let end = (at + 48).min(src.len());
            src.get(at..end).unwrap_or("<not a char boundary>")
        }
        /// Length in bytes of the char literal starting at `at` (which must be
        /// the opening `'`), or `None` when this is not a char literal — a
        /// lifetime, or a label. Handles `'x'` and `'\x'`; a multi-byte char is
        /// measured by finding the closing quote rather than assuming one byte.
        fn char_literal_len(bytes: &[u8], at: usize) -> Option<usize> {
            let escaped = bytes.get(at + 1) == Some(&b'\\');
            let body_start = if escaped { at + 2 } else { at + 1 };
            // A char literal's body is one char, so the close quote is within a
            // few bytes; bounding the search is what stops a lifetime followed
            // by an unrelated quote from being swallowed.
            for (end, byte) in bytes.iter().enumerate().skip(body_start).take(4) {
                if *byte == b'\'' {
                    return (end > body_start).then_some(end - at + 1);
                }
            }
            None
        }
        let bytes = src.as_bytes();
        let mut out = String::with_capacity(src.len());
        let mut i = 0usize;
        while i < bytes.len() {
            match bytes[i] {
                b'r' if bytes[i + 1..].starts_with(b"\"") || bytes[i + 1..].starts_with(b"#") => {
                    panic!(
                        "blank_literals cannot mask a raw string, so the brace count \
                         it feeds would be wrong and the scrape would silently cover \
                         the wrong region. EXTEND this function to handle raw strings; \
                         do not delete the call. At byte {i} of the scraped region: {:?}",
                        excerpt(src, i)
                    );
                }
                b'/' if bytes[i + 1..].starts_with(b"*") => {
                    panic!(
                        "blank_literals cannot mask a block comment, so the brace count \
                         it feeds would be wrong and the scrape would silently cover \
                         the wrong region. EXTEND this function to handle block \
                         comments; do not delete the call. At byte {i} of the scraped \
                         region: {:?}",
                        excerpt(src, i)
                    );
                }
                b'/' if bytes[i + 1..].starts_with(b"/") => {
                    while i < bytes.len() && bytes[i] != b'\n' {
                        out.push(' ');
                        i += 1;
                    }
                }
                b'"' => {
                    out.push(' ');
                    i += 1;
                    while i < bytes.len() && bytes[i] != b'"' {
                        if bytes[i] == b'\\' {
                            out.push(' ');
                            i += 1;
                        }
                        if i < bytes.len() {
                            out.push(' ');
                            i += 1;
                        }
                    }
                    assert!(i < bytes.len(), "unterminated string literal");
                    out.push(' ');
                    i += 1;
                }
                // A char literal, `'x'` or `b'x'`, for ANY x — not just a brace.
                //
                // Matching only `'{'`/`'}'` here was a real bug with exactly the
                // shape this function exists to prevent: `'"'` fell through to
                // the `_` arm, its quote was pushed, and the NEXT iteration read
                // that quote as a string opener and blanked everything to the
                // following `"` in the file. Measured on `'"'` inserted into
                // `prepare_contract_call_inner`: the scraped region grew from
                // 5,389 to 21,441 bytes, swallowing three later functions, with
                // every assertion still green.
                //
                // `\\`-escaped forms (`'\''`, `'\\'`, `'\n'`) are covered by the
                // escape branch. A LIFETIME (`'a`, `'static`) is not matched,
                // because it has no closing quote in the checked position.
                b'\'' if char_literal_len(bytes, i).is_some() => {
                    let len = char_literal_len(bytes, i).expect("just checked");
                    for _ in 0..len {
                        out.push(' ');
                    }
                    i += len;
                }
                _ => {
                    let ch = src[i..].chars().next().expect("in bounds");
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
        }
        debug_assert_eq!(out.len(), src.len(), "blank_literals must preserve offsets");
        out
    }

    /// A brace inside a string or char literal is not structure.
    #[test]
    fn braces_inside_literals_are_not_counted_as_structure() {
        let masked = blank_literals("{ f(\"}}}{\"); g('{'); }");
        assert_eq!(
            masked.matches('{').count(),
            1,
            "a brace inside a literal was counted as structure: {masked}"
        );
        assert_eq!(masked.matches('}').count(), 1, "{masked}");
        assert_eq!(
            masked.len(),
            "{ f(\"}}}{\"); g('{'); }".len(),
            "the mask changed byte offsets, so they no longer index the original"
        );
    }

    /// A brace in a comment is not structure, and an escaped quote does not end
    /// the literal it is inside.
    #[test]
    fn comments_and_escaped_quotes_are_handled() {
        let masked = blank_literals("{ // }}}\n f(\"a\\\"}\"); }");
        assert_eq!(masked.matches('{').count(), 1, "{masked}");
        assert_eq!(masked.matches('}').count(), 1, "{masked}");
    }

    /// A lifetime is not a char literal.
    #[test]
    fn a_lifetime_is_not_mistaken_for_a_char_literal() {
        let src = "{ fn f<'a>(x: &'a str) -> &'a str { x } }";
        assert_eq!(blank_literals(src), src);
    }

    /// A char literal holding a QUOTE is masked, not treated as a string opener.
    ///
    /// The arm used to match only `'{'` and `'}'`; `'"'` fell through to `_`,
    /// its quote was pushed, and the next iteration read that quote as a string
    /// opener and blanked everything to the following `"` in the file. Measured
    /// before the fix: inserting `let _q = '"';` into `prepare_contract_call_inner`
    /// grew its scraped region from 5,389 to 21,441 bytes — three whole functions
    /// — with all 26 tests still green. Precisely the silent widening this
    /// function exists to prevent.
    #[test]
    fn a_char_literal_holding_a_quote_does_not_open_a_string() {
        let masked = blank_literals("{ let _q = '\"'; f(); }");
        assert_eq!(
            masked.matches('{').count(),
            1,
            "structure was lost after a quote char literal: {masked}"
        );
        assert_eq!(masked.matches('}').count(), 1, "{masked}");
        assert!(
            masked.contains("f()"),
            "the code after a quote char literal was blanked as if it were \
             inside a string: {masked}"
        );
        assert_eq!(masked.len(), "{ let _q = '\"'; f(); }".len());
    }

    /// The byte-string form of the same trap.
    #[test]
    fn a_byte_char_literal_holding_a_quote_does_not_open_a_string() {
        let masked = blank_literals("{ if c == b'\"' { g(); } }");
        assert_eq!(
            masked.matches('{').count(),
            2,
            "structure was lost after a byte quote literal: {masked}"
        );
        assert_eq!(masked.matches('}').count(), 2, "{masked}");
    }

    /// Escaped char literals are masked whole, so the escaped quote in `'\''`
    /// does not leak either.
    #[test]
    fn escaped_char_literals_are_masked_whole() {
        let masked = blank_literals("{ a('\\''); b('\\\\'); c('\\n'); d(); }");
        assert_eq!(masked.matches('{').count(), 1, "{masked}");
        assert_eq!(masked.matches('}').count(), 1, "{masked}");
        assert!(masked.contains("d()"), "code after was blanked: {masked}");
    }

    /// Char literals other than braces and quotes are masked too, and masking
    /// them must not disturb the surrounding structure.
    #[test]
    fn ordinary_char_literals_are_masked_without_losing_structure() {
        let src = "{ m(' '); n('x'); o('é'); }";
        let masked = blank_literals(src);
        assert_eq!(masked.matches('{').count(), 1, "{masked}");
        assert_eq!(masked.matches('}').count(), 1, "{masked}");
        assert_eq!(
            masked.len(),
            src.len(),
            "masking a multi-byte char literal changed byte offsets"
        );
    }

    #[test]
    #[should_panic(expected = "raw string")]
    fn a_raw_string_fails_closed() {
        blank_literals("{ let s = r\"}{\"; }");
    }

    #[test]
    #[should_panic(expected = "block comment")]
    fn a_block_comment_fails_closed() {
        blank_literals("{ /* } */ }");
    }

    /// The two copies of `blank_literals` have not drifted apart.
    ///
    /// It exists in this crate and in `freenet`'s call-site pin. Sharing one
    /// copy is not available: both live in `#[cfg(test)]` modules, and
    /// `#[cfg(test)]` code is not compiled into the library `fdev` links, so
    /// sharing would mean exposing a test-only helper on `freenet`'s public
    /// surface (or gating it behind the `testing` feature and adding that edge
    /// to `fdev`'s dev-dependencies) purely to avoid a duplicate.
    ///
    /// Duplication is the lesser cost, but only with this pin — because the way
    /// two maskers drift is that one stops masking something, which makes its
    /// scrape silently cover the wrong region while its own tests still pass.
    /// That is precisely the failure class both pins exist to prevent, so it
    /// must not become the failure class of the pins themselves.
    ///
    /// This crate is the right home for the check: it can read `freenet`'s
    /// source, whereas a test inside `freenet` scraping its own file could be
    /// satisfied by this very assertion's text. Byte equality is the assertion,
    /// so an edit applied to BOTH copies passes — that is the correct
    /// remediation, not a hole.
    #[test]
    fn the_two_blank_literals_have_not_drifted() {
        const START: &str = "    fn blank_literals(src: &str) -> String {\n";
        const END: &str = "        out\n    }\n";

        fn extract(src: &str, whose: &str) -> String {
            let start = src.find(START).unwrap_or_else(|| {
                panic!(
                    "{whose} has no `blank_literals`; if it was renamed or removed, \
                     this pin must be updated, not deleted"
                )
            });
            let rest = &src[start..];
            let end = rest
                .find(END)
                .unwrap_or_else(|| panic!("{whose}'s `blank_literals` does not end as expected"))
                + END.len();
            rest[..end].to_string()
        }

        let ours = extract(include_str!("conformance.rs"), "fdev");
        let theirs = extract(
            include_str!("../../core/src/wasm_runtime/runtime.rs"),
            "freenet's runtime.rs",
        );

        // Non-vacuity: whatever was extracted must be the real function, so a
        // mis-anchored scrape cannot compare two empty strings and pass.
        for (whose, body) in [("fdev", &ours), ("freenet", &theirs)] {
            assert!(
                body.contains("cannot mask a raw string")
                    && body.contains("cannot mask a block comment")
                    && body.contains("unterminated string literal"),
                "the extracted `blank_literals` from {whose} is not the real \
                 function:\n{body}"
            );
        }

        assert_eq!(
            ours, theirs,
            "the two copies of `blank_literals` have drifted. One of them now \
             masks something the other does not, which means one of the two \
             source scrapes is silently covering the wrong region while its own \
             tests still pass. Re-sync them byte for byte."
        );
    }

    /// A function's body with whole-line comments stripped, so a comment mentioning
    /// the searched-for token can neither satisfy nor defeat the assertion. The
    /// comment above the `eprintln!` call names both macros; the comment above the
    /// report's no-evidence blocks named both fields the old pin looked for.
    ///
    /// Takes the signature rather than hard-coding one, so that the comment-stripping
    /// version is the ONLY way to scrape a body in this file.
    pub(super) fn code_only(signature: &str) -> String {
        fn_body(signature)
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
        code_only("fn load_inputs(")
            .replace("eprintln!", "")
            .replace("eprint!", "")
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

    /// The standalone code-diagnostic report goes to stderr, never stdout.
    ///
    /// Its own rustdoc states the invariant — stdout may be a `--json` document
    /// a consumer is parsing — but nothing enforced it: changing `eprint!` to
    /// `print!` left all 101 fdev tests green. `load_inputs_never_writes_to_stdout`
    /// is scoped to `fn load_inputs(` and does not reach this function.
    ///
    /// Both halves matter. The negative half alone would pass if the output
    /// were deleted outright, which silences the diagnostic instead of
    /// misplacing it.
    #[test]
    fn the_standalone_report_writes_to_stderr_only() {
        let raw = code_only("fn report_code_diagnostics_standalone(");
        assert!(
            raw.contains("eprint!"),
            "the standalone report no longer writes anything, so the paths that \
             abort before a Report exists say nothing at all:\n{raw}"
        );
        // `eprint!`/`eprintln!` CONTAIN `print!`/`println!` as substrings, so
        // they must be removed before the negative assertion — the same trap
        // `stdout_macros_only` documents just above.
        let stdout_only = raw.replace("eprintln!", "").replace("eprint!", "");
        assert!(
            !stdout_only.contains("print!") && !stdout_only.contains("println!"),
            "the standalone report writes to stdout, which lands in the middle \
             of a --json document and corrupts it for every consumer that parses \
             stdout:\n{raw}"
        );
    }

    /// The no-corpus bail reports code diagnostics BEFORE it aborts.
    ///
    /// `fdev verify-merge --wasm mycontract.wasm` is the invocation this is
    /// about; see
    /// `tests::the_plain_wasm_only_invocation_bails_on_a_contract_that_has_something_to_report`
    /// for the behavioural half. The guards in `conformance()` cannot cover it,
    /// because this bail short-circuits `load_inputs(&config)?` before any of
    /// them run.
    ///
    /// Mutation this exists for: move the report call below the `bail!`, or
    /// delete it. Every other test in both crates stays green.
    #[test]
    fn the_no_corpus_bail_reports_diagnostics_first() {
        let body = code_only("fn load_inputs(");
        let reported = body.find("report_code_diagnostics_standalone(").expect(
            "the no-corpus bail no longer reports code diagnostics, so \
                     `--wasm` with no corpus tells an author nothing about the clock",
        );
        let bail = body
            .find("at least one --state or --transition")
            .expect("the no-corpus bail is gone; re-check what this pin guards");
        assert!(
            reported < bail,
            "code diagnostics are reported after the bail that aborts the run, \
             so they are never reached:\n{body}"
        );
        assert_eq!(
            body.matches("report_code_diagnostics_standalone(").count(),
            1,
            "expected exactly one report call in `load_inputs`:\n{body}"
        );
    }

    #[test]
    fn the_bundle_note_still_reaches_the_reader() {
        let body = code_only("fn load_inputs(");
        assert!(
            body.contains("eprintln!"),
            "the bundle note is no longer surfaced at all; a corpus whose related \
             state was refused would replay as a clean bill of health"
        );
    }
}

/// Source pin: the host-clock code diagnostic is actually computed by the command.
///
/// `code_diagnostics` is unit-tested above, but a diagnostic nothing calls is a
/// diagnostic nobody sees — and deleting the call leaves every one of those unit
/// tests green. Running the real command in a test needs a compiled contract, a
/// corpus and a WASM runtime, which is a large fixture to guard one call, so the
/// call is pinned at the source level instead.
#[cfg(test)]
mod host_clock_diagnostic_pin {
    use super::stdout_purity_pin::code_only;

    #[test]
    fn the_command_computes_code_diagnostics() {
        let body = code_only("pub async fn conformance(");
        assert_eq!(
            body.matches("code_diagnostics(&wasm)").count(),
            1,
            "`fdev verify-merge` no longer computes code diagnostics from the \
             contract's WASM, so a clock-reading contract is reported as clean:\n{body}"
        );
    }

    /// The pin above is only worth anything if its scrape can fail. An empty or
    /// mis-anchored region would make it vacuous rather than false.
    #[test]
    fn the_scrape_sees_real_code() {
        let body = code_only("pub async fn conformance(");
        assert!(
            body.contains("Report::build("),
            "the scraped region is not `conformance`'s body any more:\n{body}"
        );
    }

    /// The diagnostic must be computed BEFORE the guards that abort without a
    /// report, or the cheapest answer the command has stays gated behind having
    /// a workable corpus — which is exactly what an author early in development
    /// does not have.
    ///
    /// Mutation this exists for: move `let code_diagnostics = ...` back below
    /// the corpus/cases guards. Every other test in this file stays green.
    #[test]
    fn diagnostics_are_computed_before_the_guards_that_abort() {
        let body = code_only("pub async fn conformance(");
        let computed = body
            .find("code_diagnostics(&wasm)")
            .expect("conformance no longer computes code diagnostics");
        let corpus_guard = body
            .find("corpus.is_empty()")
            .expect("the empty-corpus guard is gone; re-check what this pin is guarding");
        let cases_guard = body
            .find("cases.is_empty()")
            .expect("the no-cases guard is gone; re-check what this pin is guarding");
        assert!(
            computed < corpus_guard && computed < cases_guard,
            "code diagnostics are computed after a guard that aborts, so an author \
             whose corpus cannot produce a case is told nothing about the clock"
        );
        assert_eq!(
            body.matches("report_code_diagnostics_standalone(").count(),
            2,
            "both aborting guards must report the diagnostics they are about to \
             skip past; found a different number of call sites:\n{body}"
        );
    }

    /// `--evidence` returns before any of the above runs, and
    /// `conformance_log_level` returns `OFF` for `--json`, so without this call
    /// that mode reports the clock nowhere at all.
    #[test]
    fn verifying_evidence_also_reports_code_diagnostics() {
        let body = code_only("async fn verify_evidence(");
        assert_eq!(
            body.matches("report_code_diagnostics_standalone(").count(),
            1,
            "`--evidence` no longer reports code diagnostics:\n{body}"
        );
        assert!(
            body.contains("RuntimeOracle::standalone("),
            "the scraped region is not `verify_evidence`'s body any more:\n{body}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet::conformance::{OracleError, Violation};
    use freenet_stdlib::prelude::{RelatedContracts, UpdateModification, ValidateResult};

    /// Render the human report to a string.
    ///
    /// The report's text is what these tests assert on, rather than its source: a
    /// source scrape of `print_human` is satisfied by a comment naming the same
    /// tokens, which is exactly how the previous version of
    /// `the_human_report_says_why_no_evidence_was_written` survived the deletion of
    /// both blocks it existed to guard.
    fn render_human(report: &Report) -> String {
        let mut rendered = Vec::new();
        report
            .write_human(&mut rendered)
            .expect("writing to a Vec cannot fail");
        String::from_utf8(rendered).expect("the report is utf-8")
    }

    /// A grow-only union contract, enough to exercise the reversed-pair heuristic
    /// without a WASM runtime.
    struct GrowOnly;

    impl ConformanceOracle for GrowOnly {
        fn validate_state(
            &mut self,
            _state: &[u8],
            _related: &freenet_stdlib::prelude::RelatedContracts<'_>,
        ) -> Result<freenet_stdlib::prelude::ValidateResult, freenet::conformance::OracleError>
        {
            Ok(freenet_stdlib::prelude::ValidateResult::Valid)
        }

        fn update_state(
            &mut self,
            state: &[u8],
            updates: &[UpdateData<'_>],
        ) -> Result<
            freenet_stdlib::prelude::UpdateModification<'static>,
            freenet::conformance::OracleError,
        > {
            let mut out = state.to_vec();
            for update in updates {
                match update {
                    UpdateData::State(incoming) => out.extend_from_slice(incoming.as_ref()),
                    UpdateData::Delta(delta) => out.extend_from_slice(delta.as_ref()),
                    _ => {}
                }
            }
            out.sort_unstable();
            out.dedup();
            Ok(freenet_stdlib::prelude::UpdateModification::valid(
                State::from(out),
            ))
        }

        fn summarize_state(
            &mut self,
            state: &[u8],
        ) -> Result<Vec<u8>, freenet::conformance::OracleError> {
            Ok(state.to_vec())
        }

        fn get_state_delta(
            &mut self,
            state: &[u8],
            summary: &[u8],
        ) -> Result<Vec<u8>, freenet::conformance::OracleError> {
            Ok(state
                .iter()
                .copied()
                .filter(|b| !summary.contains(b))
                .collect())
        }
    }

    /// A reversed `--transition BASE RESULT` pair is the one malformed input this
    /// command cannot turn into an error, so it must at least be warned about.
    ///
    /// Argument order is the entire provenance `transition_path_agreement` rests on
    /// when a human supplies the pair. Given the two paths the wrong way round, a
    /// perfectly conforming grow-only contract is reported as violating the law and
    /// nothing distinguishes that from a real finding — the failure is silent and
    /// confident, which is the worst combination.
    #[test]
    fn a_reversed_transition_pair_is_warned_about_and_a_correct_one_is_not() {
        let earlier = Bytes::from(vec![1u8, 2]);
        let later = Bytes::from(vec![1u8, 2, 3]);

        assert_eq!(
            warn_on_reversed_transitions(&mut GrowOnly, &[(earlier.clone(), later.clone())]),
            0,
            "a correctly ordered pair must not be warned about, or the warning is \
             noise on every run and stops being read"
        );
        assert_eq!(
            warn_on_reversed_transitions(&mut GrowOnly, &[(later, earlier)]),
            1,
            "merging BASE into RESULT reproducing BASE is what a reversed pair \
             looks like, and it is the only tell there is"
        );
    }

    /// The `--transition` arity invariant is an error, not a panic.
    ///
    /// `num_args = 2` makes it unreachable through clap today; the point is that
    /// loosening the arg definition later must not turn a user's mistake into a
    /// backtrace, and must not silently drop a lone path either.
    #[test]
    fn an_odd_number_of_transition_paths_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wasm = dir.path().join("c.wasm");
        // Never loaded: the arity check is reached long before any runtime exists.
        std::fs::write(&wasm, b"\0asm-stand-in").expect("write wasm");
        let config = ConformanceConfig {
            wasm: Some(wasm),
            params: None,
            states: Vec::new(),
            transitions: vec![PathBuf::from("only-one.bin")],
            bundle: None,
            contract_store: None,
            max_cases: None,
            properties: Vec::new(),
            json: false,
            evidence_out: None,
            evidence_in: None,
            bundle_out: None,
        };
        let err = load_inputs(&config).expect_err("an odd count must be rejected");
        assert!(
            err.to_string().contains("--transition takes two paths"),
            "the error must name the argument, got: {err}"
        );
    }

    /// `--bundle-out` must re-export a corpus that replays to the SAME corpus.
    ///
    /// Not just the same states and steps: the same `delta_bases`. A `Transition` is
    /// the only place a bundle can record what a delta was applied to, because
    /// `ReplayBundle::to_corpus` deliberately gives bundle-level deltas no base at
    /// all — pairing causally sequenced deltas asks about a situation the protocol
    /// never produces. So re-exporting a transition without its `delta` drops every
    /// pairing, and `delta_permutation_invariance` — which pairs only deltas
    /// observed against the SAME base — checks nothing on the replayed bundle while
    /// the original run checked plenty.
    ///
    /// That is the worst shape this command can have: the replay reports a clean run
    /// and reads as a reproduction of the original.
    #[test]
    fn a_re_exported_bundle_keeps_what_each_delta_was_applied_to() {
        let code = b"\0asm-stand-in".to_vec();
        let base = vec![1u8, 2];
        // Two deltas against the SAME base: the pairing this exists to preserve.
        let bundle_in = ReplayBundle {
            transitions: vec![
                Transition {
                    base_state: base.clone(),
                    result_state: vec![1, 2, 3],
                    delta: Some(vec![3]),
                    ..Default::default()
                },
                Transition {
                    base_state: base.clone(),
                    result_state: vec![1, 2, 4],
                    delta: Some(vec![4]),
                    ..Default::default()
                },
            ],
            ..ReplayBundle::new(code.clone(), Vec::new())
        };

        let original = bundle_in.to_corpus();
        assert_eq!(
            original.delta_bases,
            vec![
                Some(Bytes::from(base.clone())),
                Some(Bytes::from(base.clone()))
            ],
            "the fixture must actually carry provenance, or the round trip below \
             has nothing to lose"
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.bin");
        write_bundle(&path, &code, &[], &original).expect("write bundle");
        let round_tripped = ReplayBundle::read_from(&path)
            .expect("read bundle")
            .to_corpus();

        assert_eq!(round_tripped.transitions, original.transitions);
        assert_eq!(round_tripped.deltas, original.deltas);
        assert_eq!(
            round_tripped.delta_bases, original.delta_bases,
            "a re-exported bundle must pair each delta with the state it was \
             applied to, exactly as the corpus it was written from did"
        );
        assert_eq!(round_tripped.states, original.states);
    }

    /// The same shape again, where the two steps COLLAPSE into one.
    ///
    /// The test above pairs two same-base deltas that arrive on two distinct steps,
    /// so one delta per step is enough to carry them. `Corpus::deduplicated` keys
    /// steps on `(base, result)`, so two updates taken from the same state that land
    /// on the same result become ONE step holding two same-base deltas — and a
    /// `Transition` holds at most one delta. Assigning one and chaining the rest onto
    /// `bundle.deltas` sent the second out loose, where `to_corpus` gives it no base
    /// and `delta_permutation_invariance` never pairs it.
    ///
    /// A same-base pair is exactly what that property is FOR, so the re-export
    /// silently checked less than the run it replayed while reporting a clean bill of
    /// health. The fix repeats the step once per further delta; the duplicate
    /// `(base, result)` pairs collapse again on the way back in.
    #[test]
    fn a_re_exported_bundle_keeps_every_delta_of_a_collapsed_step() {
        let code = b"\0asm-stand-in".to_vec();
        let base = vec![1u8, 2];
        let result = vec![1u8, 2, 3];
        // Same base AND same result, two different deltas: one step after dedup.
        let bundle_in = ReplayBundle {
            transitions: vec![
                Transition {
                    base_state: base.clone(),
                    result_state: result.clone(),
                    delta: Some(vec![3]),
                    ..Default::default()
                },
                Transition {
                    base_state: base.clone(),
                    result_state: result.clone(),
                    delta: Some(vec![4]),
                    ..Default::default()
                },
            ],
            ..ReplayBundle::new(code.clone(), Vec::new())
        };

        let original = bundle_in.to_corpus();
        assert_eq!(
            original.transitions.len(),
            1,
            "the fixture must actually collapse to one step, or it is the test above"
        );
        assert_eq!(
            original.delta_bases,
            vec![
                Some(Bytes::from(base.clone())),
                Some(Bytes::from(base.clone()))
            ],
            "and both deltas must start out provenanced, or the round trip below has \
             nothing to lose"
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.bin");
        write_bundle(&path, &code, &[], &original).expect("write bundle");
        let round_tripped = ReplayBundle::read_from(&path)
            .expect("read bundle")
            .to_corpus();

        assert_eq!(round_tripped.transitions, original.transitions);
        assert_eq!(round_tripped.deltas, original.deltas);
        assert_eq!(
            round_tripped.delta_bases, original.delta_bases,
            "the second delta of a collapsed step must keep its base too, or the \
             one pairing this corpus can support is gone from the replay"
        );
        assert_eq!(round_tripped.states, original.states);
    }

    /// An oracle that records every call, so a test can ask whether the WASM path
    /// was entered at all.
    ///
    /// Deliberately trivial otherwise: nothing here is checking merge behaviour, only
    /// whether `write_evidence` decided to spend WASM calls on a case it was going to
    /// discard.
    #[derive(Default)]
    struct CountingOracle {
        calls: std::cell::Cell<usize>,
    }

    impl freenet::conformance::ConformanceOracle for CountingOracle {
        fn validate_state(
            &mut self,
            _state: &[u8],
            _related: &RelatedContracts<'_>,
        ) -> Result<ValidateResult, OracleError> {
            self.calls.set(self.calls.get() + 1);
            Ok(ValidateResult::Valid)
        }

        fn update_state(
            &mut self,
            state: &[u8],
            _updates: &[UpdateData<'_>],
        ) -> Result<UpdateModification<'static>, OracleError> {
            self.calls.set(self.calls.get() + 1);
            Ok(UpdateModification::valid(State::from(state.to_vec())))
        }

        fn summarize_state(&mut self, _state: &[u8]) -> Result<Vec<u8>, OracleError> {
            self.calls.set(self.calls.get() + 1);
            Ok(Vec::new())
        }

        fn get_state_delta(
            &mut self,
            _state: &[u8],
            _summary: &[u8],
        ) -> Result<Vec<u8>, OracleError> {
            self.calls.set(self.calls.get() + 1);
            Ok(Vec::new())
        }
    }

    /// A finding that can never become evidence must be discarded BEFORE minimisation.
    ///
    /// Two things go wrong when the check sits after `minimize`, and they are not the
    /// same defect. The cheap one is waste: minimisation is the expensive part of this
    /// loop, and every byte of it is spent on a case that is then thrown away. The
    /// one that misleads a reader is `input_bytes_before_shrinking`, documented as the
    /// measure of "whether shrinking is earning its keep" — accumulating for a finding
    /// shrinking could not have helped puts a number in the denominator the ratio is
    /// not about. Measured on a real run before the fix: `files_written: 0` alongside
    /// `input_bytes_before_shrinking: 32335`.
    #[test]
    fn a_local_only_finding_costs_no_minimisation_and_no_shrink_bytes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let out = dir.path().join("evidence");
        let mut oracle = CountingOracle::default();

        // `TransitionPathAgreement` is `Violation` (so it reaches the loop body) and
        // `LocalProvenance` (so it can never be written).
        let property = ConformanceProperty::TransitionPathAgreement;
        assert!(
            !property.is_self_verifying() && property.severity() == Severity::Violation,
            "the fixture depends on this property being enforceable AND local-only"
        );
        let case = ConformanceCase::new(
            property,
            vec![Bytes::from(vec![7u8; 64]), Bytes::from(vec![8u8; 64])],
        );
        assert!(
            case.input_bytes() > 0,
            "the case must carry bytes, or the counter assertion below is vacuous"
        );

        let summary = write_evidence(
            &out,
            ContractInstanceId::new([3u8; 32]),
            &[],
            &[(case, PropertyOutcome::Violated(violation_of(property)))],
            &mut oracle,
            &[],
            &[],
        )
        .expect("write evidence");

        assert_eq!(summary.files_written, 0);
        assert_eq!(summary.findings_local_only, 1);
        assert_eq!(summary.findings_too_large, 0);
        assert_eq!(
            oracle.calls.get(),
            0,
            "minimisation and re-verification must not run for a finding that cannot \
             travel; they are the expensive part of this loop"
        );
        assert_eq!(
            (
                summary.input_bytes_before_shrinking,
                summary.input_bytes_after_shrinking
            ),
            (0, 0),
            "and its bytes must not land in the shrink ratio, whose denominator is \
             supposed to be work shrinking was asked to do"
        );
    }

    /// The counterpart: a shippable finding DOES pay for minimisation and IS counted.
    ///
    /// Without this, moving the local-only check to the top of the loop body — or
    /// deleting `minimize` outright — would satisfy the test above.
    #[test]
    fn a_shippable_finding_is_minimised_and_counted() {
        let dir = tempfile::tempdir().expect("temp dir");
        let out = dir.path().join("evidence");
        let mut oracle = CountingOracle::default();

        let property = ConformanceProperty::StateCommutativity;
        assert!(property.is_self_verifying() && property.severity() == Severity::Violation);
        let case = ConformanceCase::new(
            property,
            vec![Bytes::from(vec![7u8; 64]), Bytes::from(vec![8u8; 64])],
        );

        let summary = write_evidence(
            &out,
            ContractInstanceId::new([3u8; 32]),
            &[],
            &[(case, PropertyOutcome::Violated(violation_of(property)))],
            &mut oracle,
            &[],
            &[],
        )
        .expect("write evidence");

        assert_eq!(summary.files_written, 1);
        assert_eq!(summary.findings_local_only, 0);
        assert!(
            oracle.calls.get() > 0,
            "a shippable finding must actually go through minimisation"
        );
        assert_eq!(summary.input_bytes_before_shrinking, 128);
    }

    /// "wrote 0 evidence file(s)" must never stand alone.
    ///
    /// `findings_too_large`'s own doc comment says silently writing nothing would look
    /// like there were none — and that is precisely what the DEFAULT (non-`--json`)
    /// output did for both no-file reasons. The person who most needs the explanation
    /// is the one who did not ask for JSON.
    #[test]
    fn the_human_report_says_why_no_evidence_was_written() {
        let report = Report {
            corpus_states: 1,
            corpus_deltas: 1,
            corpus_summaries: 0,
            cases_run: 2,
            holds: 0,
            violations: 2,
            enforceable_violations: 2,
            diagnostic_violations: 0,
            inconclusive: 0,
            findings: Vec::new(),
            inconclusive_reasons: Vec::new(),
            code_diagnostics: Vec::new(),
            evidence: Some(EvidenceSummary {
                directory: "/tmp/evidence".to_string(),
                files_written: 0,
                findings_local_only: 1,
                findings_too_large: 1,
                input_bytes_before_shrinking: 0,
                input_bytes_after_shrinking: 0,
            }),
        };

        let rendered = render_human(&report);

        assert!(
            rendered.contains("wrote 0 evidence file(s)"),
            "the fixture no longer produces the line this pin is about:\n{rendered}"
        );
        assert!(
            rendered.contains("the evidence bytes cannot carry"),
            "a local-only finding drew no explanation, so the report says \
             'wrote 0 evidence file(s)' and nothing else — which reads exactly \
             like a clean run:\n{rendered}"
        );
        assert!(
            rendered.contains("stayed over the evidence size limit"),
            "an over-size finding drew no explanation, so the report says \
             'wrote 0 evidence file(s)' and nothing else — which reads exactly \
             like a clean run:\n{rendered}"
        );
    }

    /// Twenty-four cases breaking one law are ONE note, not twenty-four.
    ///
    /// Both notes fired once per violated CASE, and a corpus routinely produces dozens
    /// of cases of the same defect — `group_findings` exists for that reason on the
    /// report side. The identical lines then bury the report the note points at.
    #[test]
    fn the_unwritable_notes_are_one_line_per_property_not_one_per_case() {
        let mut local_only = Vec::new();
        for _ in 0..24 {
            note_unwritable(
                &mut local_only,
                ConformanceProperty::TransitionPathAgreement,
                None,
            );
        }
        note_unwritable(
            &mut local_only,
            ConformanceProperty::DeltaPermutationInvariance,
            None,
        );

        let mut oversized = Vec::new();
        for found in [900usize, 800, 700] {
            note_unwritable(
                &mut oversized,
                ConformanceProperty::StateCommutativity,
                Some(format!("{found} bytes over")),
            );
        }

        let mut rendered = Vec::new();
        write_unwritable_notes(&mut rendered, &local_only, &oversized)
            .expect("writing to a Vec cannot fail");
        let rendered = String::from_utf8(rendered).expect("notes are utf-8");

        assert_eq!(
            rendered.lines().count(),
            3,
            "28 unwritable findings across 3 properties should print 3 lines:\n{rendered}"
        );
        assert!(
            rendered.contains("24 transition_path_agreement finding(s)"),
            "the note does not say how many cases it stands for:\n{rendered}"
        );
        assert!(
            rendered.contains("1 delta_permutation_invariance finding(s)"),
            "a second property's findings were folded into the first's note:\n{rendered}"
        );
        // The FIRST example, so a re-run producing the same findings prints the same
        // line rather than whichever case happened to land last.
        assert!(
            rendered.contains("example: 900 bytes over"),
            "the over-size note lost its representative rejection:\n{rendered}"
        );
    }

    /// The counterpart: a run with no unwritable findings must print NEITHER line.
    ///
    /// Without this, the pin above is satisfied by emitting both explanations
    /// unconditionally, which would tell every clean run that findings it does not
    /// have could not be written.
    #[test]
    fn the_no_evidence_explanations_are_conditional() {
        let report = Report {
            corpus_states: 1,
            corpus_deltas: 1,
            corpus_summaries: 0,
            cases_run: 1,
            holds: 1,
            violations: 0,
            enforceable_violations: 0,
            diagnostic_violations: 0,
            inconclusive: 0,
            findings: Vec::new(),
            inconclusive_reasons: Vec::new(),
            code_diagnostics: Vec::new(),
            evidence: Some(EvidenceSummary {
                directory: "/tmp/evidence".to_string(),
                files_written: 0,
                findings_local_only: 0,
                findings_too_large: 0,
                input_bytes_before_shrinking: 0,
                input_bytes_after_shrinking: 0,
            }),
        };

        let rendered = render_human(&report);

        assert!(
            !rendered.contains("the evidence bytes cannot carry")
                && !rendered.contains("stayed over the evidence size limit"),
            "a run with zero unwritable findings explained away findings it does \
             not have:\n{rendered}"
        );
    }

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

    /// Assemble a module importing one function per `(namespace, name)` pair.
    fn module_importing(imports: &[(&str, &str)]) -> Vec<u8> {
        let mut wat = String::from("(module\n");
        for (i, (namespace, name)) in imports.iter().enumerate() {
            wat.push_str(&format!(
                "  (import \"{namespace}\" \"{name}\" (func $f{i} (param i64 i64)))\n"
            ));
        }
        wat.push_str(")\n");
        wat::parse_str(&wat).expect("test fixture is valid wat")
    }

    fn report_with_code_diagnostics(diagnostics: Vec<CodeDiagnostic>) -> Report {
        Report {
            corpus_states: 2,
            corpus_deltas: 0,
            corpus_summaries: 0,
            cases_run: 4,
            holds: 4,
            violations: 0,
            enforceable_violations: 0,
            diagnostic_violations: 0,
            inconclusive: 0,
            findings: Vec::new(),
            inconclusive_reasons: Vec::new(),
            evidence: None,
            code_diagnostics: diagnostics,
        }
    }

    /// A contract that reads the host clock must be reported (issue #5465): its
    /// merge is not a function of its inputs, so the laws checked above are not
    /// well-formed statements about it.
    #[test]
    fn a_clock_importing_contract_draws_a_code_diagnostic() {
        let wasm = module_importing(&[(
            host_clock::HOST_CLOCK_NAMESPACE,
            host_clock::HOST_CLOCK_IMPORT,
        )]);
        let diagnostics = code_diagnostics(&wasm);
        assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
        assert_eq!(diagnostics[0].diagnostic, "host_clock_import");
    }

    /// The counterpart. Without this, emitting the diagnostic unconditionally
    /// passes the test above while telling every contract author that their
    /// contract reads a clock it never touches.
    #[test]
    fn a_contract_that_reads_no_clock_draws_no_code_diagnostic() {
        let wasm = module_importing(&[("freenet_log", "__frnt__logger__info")]);
        assert_eq!(code_diagnostics(&wasm), Vec::new());
    }

    /// Build a report whose only outcomes are inconclusive contract errors.
    fn report_from_contract_errors(messages: &[&str]) -> Report {
        let outcomes: Vec<(ConformanceCase, PropertyOutcome)> = messages
            .iter()
            .map(|message| {
                (
                    ConformanceCase::new(
                        ConformanceProperty::StateCommutativity,
                        vec![Bytes::from(vec![1u8]), Bytes::from(vec![2u8])],
                    ),
                    PropertyOutcome::Inconclusive(Inconclusive::ContractError(
                        (*message).to_string(),
                    )),
                )
            })
            .collect();
        Report::build(&Corpus::default(), &outcomes, None, Vec::new())
    }

    use super::stdout_purity_pin::code_only;

    fn contract_error_bucket(report: &Report) -> &InconclusiveReason {
        report
            .inconclusive_reasons
            .iter()
            .find(|r| r.reason == "contract error")
            .expect("the contract-error bucket vanished from the report")
    }

    /// The text behind `contract error` must reach the report.
    ///
    /// It was carried on the variant, printed by `Inconclusive`'s own `Display`, and
    /// then discarded at the reporting layer — so the second-largest inconclusive
    /// class was undiagnosable from the tool's own output. Measured on a 64-bundle
    /// live corpus: 435 of 2,622 inconclusive cases were `contract error`, and
    /// nothing in `--json`, the human report or `RUST_LOG=debug` could say what any
    /// of them said (#5461). A reader saw a count and nothing else, which is exactly
    /// the information needed to tell one recurring bug from eight unrelated ones.
    #[test]
    fn contract_error_text_reaches_the_report() {
        let report = report_from_contract_errors(&[
            "signature verification failed",
            "signature verification failed",
            "signature verification failed",
            "stale nonce",
        ]);

        let reason = contract_error_bucket(&report);
        assert_eq!(reason.occurrences, 4);
        assert_eq!(
            reason
                .examples
                .iter()
                .map(|e| (e.text.as_str(), e.occurrences))
                .collect::<Vec<_>>(),
            vec![("signature verification failed", 3), ("stale nonce", 1)],
            "distinct messages must carry their own counts, most frequent first: \
             that breakdown is the whole answer to 'is this one bug or several'"
        );

        let rendered = render_human(&report);
        assert!(
            rendered.contains("signature verification failed"),
            "the human report still hides the contract's own error text:\n{rendered}"
        );
        assert!(
            rendered.contains("stale nonce"),
            "only the most common message survived, so a second distinct failure \
             stays invisible:\n{rendered}"
        );
    }

    /// A reason that carries no text must not grow an empty example list in the
    /// human output.
    ///
    /// Most inconclusive reasons carry nothing (`RoundLimit`, `RelatedRequired`),
    /// and they are the majority of a real run. If the new lines fired for them too,
    /// every clean-ish report would gain blank noise.
    #[test]
    fn a_textless_reason_reports_no_examples() {
        let outcomes = vec![(
            ConformanceCase::new(
                ConformanceProperty::StateCommutativity,
                vec![Bytes::from(vec![1u8])],
            ),
            PropertyOutcome::Inconclusive(Inconclusive::RoundLimit),
        )];
        let report = Report::build(&Corpus::default(), &outcomes, None, Vec::new());
        let reason = &report.inconclusive_reasons[0];
        assert!(reason.examples.is_empty());
    }

    /// The readability cap trims the HUMAN report only, and says what it hid.
    ///
    /// The cap exists because the count of distinct texts is bounded only by the
    /// case count: a contract that embeds a state hash in its error produces one per
    /// case, and a wall of them buries the question the reader has. But that is a
    /// terminal-width concern, so `--json` keeps everything - a machine consumer has
    /// no width limit, the tally already holds them all, and a capped machine format
    /// would let a hostile contract bury its real failure behind five high-frequency
    /// decoys.
    ///
    /// Capping without saying so, in either format, would reintroduce this issue's
    /// own defect in miniature: output that looks complete and is not.
    #[test]
    fn the_cap_trims_the_human_report_only_and_says_what_it_hid() {
        // Frequencies chosen so the hidden tail is a KNOWN number of cases: the
        // first five messages appear twice each, the last three once each.
        let mut messages: Vec<String> = Vec::new();
        for i in 0..MAX_INCONCLUSIVE_EXAMPLES {
            messages.push(format!("frequent failure {i}"));
            messages.push(format!("frequent failure {i}"));
        }
        for i in 0..3 {
            messages.push(format!("rare failure {i}"));
        }
        let refs: Vec<&str> = messages.iter().map(String::as_str).collect();
        let report = report_from_contract_errors(&refs);

        let reason = contract_error_bucket(&report);
        assert_eq!(
            reason.examples.len(),
            MAX_INCONCLUSIVE_EXAMPLES + 3,
            "--json must carry every distinct message; trimming it there loses data \
             that no machine consumer needed trimmed"
        );

        let rendered = render_human(&report);
        assert_eq!(
            rendered.matches("frequent failure").count(),
            MAX_INCONCLUSIVE_EXAMPLES,
            "the human report must show exactly the cap:\n{rendered}"
        );
        assert!(
            !rendered.contains("rare failure"),
            "the human report showed past its own cap:\n{rendered}"
        );
        assert!(
            rendered.contains("3 further distinct message(s), 3 case(s), not shown"),
            "the hidden tail must be reported in BOTH messages and cases: '3 further \
             messages' alone cannot distinguish 3 stray cases from 300:\n{rendered}"
        );
    }

    /// Equal counts must order deterministically, for messages and for reasons.
    ///
    /// The reason tally is drained through a `HashMap`, whose iteration order is
    /// randomly seeded per process, so two equally-frequent reasons came out in a
    /// different order on each run over the same corpus. Comparing two runs is the
    /// normal way to check whether a contract fix helped, and it is worthless
    /// against a report that reshuffles itself.
    ///
    /// The message half below is belt-and-braces: those come from a `BTreeMap`
    /// through a stable sort, so they were already ordered. The reason half is the
    /// one that was genuinely unstable.
    #[test]
    fn equal_counts_order_deterministically() {
        let report = report_from_contract_errors(&["b failure", "a failure"]);
        let texts: Vec<&str> = contract_error_bucket(&report)
            .examples
            .iter()
            .map(|e| e.text.as_str())
            .collect();
        assert_eq!(texts, vec!["a failure", "b failure"]);

        let case = || {
            ConformanceCase::new(
                ConformanceProperty::StateCommutativity,
                vec![Bytes::from(vec![1u8])],
            )
        };
        let outcomes = vec![
            (
                case(),
                PropertyOutcome::Inconclusive(Inconclusive::RoundLimit),
            ),
            (
                case(),
                PropertyOutcome::Inconclusive(Inconclusive::ContractError("boom".to_string())),
            ),
        ];
        let report = Report::build(&Corpus::default(), &outcomes, None, Vec::new());
        let reasons: Vec<&str> = report
            .inconclusive_reasons
            .iter()
            .map(|r| r.reason)
            .collect();
        assert_eq!(
            reasons,
            vec!["contract error", "reconciliation round budget exhausted"],
            "two reasons with equal counts must order by label, not by whatever \
             the hash map happened to yield on this run"
        );
    }

    /// A contract-authored message must not be able to forge report lines.
    ///
    /// This text is written by the contract under test, which is the thing we are
    /// least entitled to trust, and it is now printed into a report people read to
    /// decide whether a contract is sound. A bare newline would let it fabricate a
    /// `[violation]` line; a carriage return would let it overwrite the line above.
    #[test]
    fn a_contract_message_cannot_forge_report_lines() {
        let report =
            report_from_contract_errors(&["boom\n  [violation] StateCommutativity: forged"]);
        let rendered = render_human(&report);

        assert!(
            !rendered.contains("\n  [violation]"),
            "a newline in the contract's error text forged a findings line:\n{rendered}"
        );
        assert!(
            rendered.contains("\\n"),
            "the newline was neither escaped nor stripped:\n{rendered}"
        );
    }

    /// The work is bounded, not merely the output.
    ///
    /// `present_detail` originally collected the WHOLE escaped string and truncated
    /// afterwards. Escaping expands a control character roughly sixfold and this runs
    /// once per inconclusive CASE (deduplication happens on the result), so a hostile
    /// multi-megabyte message meant gigabytes of escaping across a run for output
    /// that was thrown away: a wall-clock denial of service against the tool.
    ///
    /// Asserted by handing the bounded half a source that panics the moment it is
    /// read past the cap. That is the only way to test laziness - the finished string
    /// looks identical either way, which is exactly why the original passed every
    /// test while doing unbounded work.
    #[test]
    fn presenting_a_detail_reads_no_further_than_the_cap() {
        let mut read = 0usize;
        let watched = std::iter::from_fn(|| {
            read += 1;
            assert!(
                read <= MAX_INCONCLUSIVE_EXAMPLE_CHARS + 1,
                "read {read} characters for a {MAX_INCONCLUSIVE_EXAMPLE_CHARS}-character \
                 cap; the escaping must stop at the cap, or an oversized contract \
                 message is expanded in full before being discarded"
            );
            Some('x')
        });

        let presented = take_presented(watched);
        assert_eq!(
            presented.chars().count(),
            MAX_INCONCLUSIVE_EXAMPLE_CHARS + 1
        );
        assert!(presented.ends_with('\u{2026}'));
    }

    /// Pin: `present_detail` hands `take_presented` a LAZY iterator.
    ///
    /// The test above proves `take_presented` reads no further than the cap. It says
    /// nothing about its caller, and a mutation that restores
    /// `text.escape_debug().collect::<Vec<char>>().into_iter()` survives it with
    /// every test green - the finished string is identical, only the work differs.
    /// That is the whole defect: unbounded work behind bounded-looking output.
    ///
    /// Sourced-scraped because laziness leaves no trace in the return value, and
    /// timing or allocation assertions would be flaky. Any `collect` in this body
    /// re-materialises the escaped string in full, so its absence is the property.
    #[test]
    fn present_detail_does_not_collect_before_bounding() {
        let body = code_only("fn present_detail(");

        assert!(
            body.contains("take_presented(text.escape_debug())"),
            "the scraped region is not present_detail's body any more:\n{body}"
        );
        assert!(
            !body.contains("collect"),
            "present_detail collects before bounding, so the whole escaped string is \
             built and thrown away: escaping expands a control character roughly \
             sixfold and this runs once per inconclusive CASE:\n{body}"
        );
    }

    /// Escapes and direction overrides are neutralised, not just newlines.
    ///
    /// `a_contract_message_cannot_forge_report_lines` checks `\n` alone, so replacing
    /// the escaping with `text.replace('\n', "\\n")` would keep it green while ESC
    /// and RLO leaked. ESC lets a contract recolour or erase the report around it;
    /// U+202E reverses the visual order of everything after it, which is enough to
    /// make a message read as a different one.
    #[test]
    fn escapes_and_direction_overrides_are_neutralised() {
        let presented = present_detail("red\u{1b}[31m and reversed \u{202e}drawrkcab");

        assert!(
            !presented.contains('\u{1b}'),
            "a raw ESC survived into the report: {presented}"
        );
        assert!(
            !presented.contains('\u{202e}'),
            "a raw right-to-left override survived into the report: {presented}"
        );
        assert!(
            presented.contains("\\u{1b}") && presented.contains("\\u{202e}"),
            "both should appear in escaped form rather than be dropped: {presented}"
        );
    }

    /// Pin: `verify_evidence` routes both untrusted strings through the escaper.
    ///
    /// This command exists to consume evidence ANOTHER PEER produced, and its own
    /// rustdoc says `evidence.observed` is never trusted - yet it interpolated the
    /// sender's `detail` straight into the terminal, and printed `Inconclusive`
    /// through a `Display` impl that embeds the contract's raw error text. A newline
    /// in either forges a `verdict  :` line in the exact format of the real one.
    ///
    /// A source pin rather than a behavioural test because reaching this function
    /// needs a WASM runtime and a real evidence file, which is a large fixture for a
    /// one-call property. It is scoped tightly and fails closed: if the function is
    /// renamed or the prints move, the scrape stops matching and the test fails.
    #[test]
    fn verify_evidence_escapes_the_text_it_does_not_trust() {
        let body = code_only("async fn verify_evidence(");

        assert!(
            body.contains("claimed  :") && body.contains("INCONCLUSIVE"),
            "the scraped region is not verify_evidence's body any more:\n{body}"
        );
        assert!(
            body.contains("present_detail(&v.detail)"),
            "the SENDER's `detail` is interpolated raw again:\n{body}"
        );
        assert!(
            body.contains("present_detail(&reason.to_string())"),
            "the `Inconclusive` rendering embeds the contract's own error text and \
             must be escaped too:\n{body}"
        );
        // The REPRODUCED branch deliberately does NOT escape: that `Violation` is
        // built locally by `verify_case`, whose `detail` values are static strings
        // plus lengths (`verifier.rs:329-482`), never contract-authored text.
        // Asserting it stays raw keeps this pin honest about which of the two is
        // trusted, so a future reader does not "fix" the wrong one.
        assert!(
            body.contains("v.property, v.detail"),
            "the locally-computed REPRODUCED line changed shape; re-check whether \
             its `detail` can now carry contract text:\n{body}"
        );
    }

    /// Truncation slices on a character boundary.
    ///
    /// Slicing a `String` at a byte offset panics when a multi-byte character
    /// straddles it, and the bytes here are chosen by the contract under test — so a
    /// byte-offset cap would be a contract-triggerable crash of the entire run,
    /// taking every other case's result with it.
    #[test]
    fn a_long_multibyte_message_is_truncated_without_panicking() {
        let presented = present_detail(&"e\u{301}".repeat(MAX_INCONCLUSIVE_EXAMPLE_CHARS * 2));
        assert_eq!(
            presented.chars().count(),
            MAX_INCONCLUSIVE_EXAMPLE_CHARS + 1,
            "expected exactly the cap plus one ellipsis: {presented}"
        );
        assert!(presented.ends_with('\u{2026}'));
    }

    /// A message at or under the cap is reported whole, with no ellipsis.
    ///
    /// The counterpart to the test above: a cap that fired on everything would
    /// satisfy it while making every message unreadable.
    #[test]
    fn a_short_message_is_not_truncated() {
        assert_eq!(present_detail("stale nonce"), "stale nonce");
    }

    /// Pin: every `Inconclusive` variant carrying text is reported by
    /// [`inconclusive_detail`].
    ///
    /// That match needs a wildcard arm, because `Inconclusive` is `#[non_exhaustive]`
    /// and this crate cannot match it exhaustively. A wildcard is precisely how the
    /// text was lost the first time, so a new text-carrying variant would land in it
    /// and be silently undiagnosable again with every test still green. Scraping the
    /// source is the only check available, since the compiler cannot be made to
    /// demand the coverage.
    ///
    /// "Carries text" is deliberately matched on the whole variant declaration rather
    /// than the literal `Name(String),` shape: `Foo(String, usize)`, `Foo(Box<str>)`
    /// and `Foo { reason: String }` all carry text and all slip past a suffix match,
    /// which would leave the pin guarding only the three variants that already exist.
    #[test]
    fn inconclusive_detail_covers_every_text_carrying_variant() {
        let enum_src = include_str!("../../core/src/conformance/property.rs");
        let start = enum_src
            .find("pub enum Inconclusive {")
            .expect("the Inconclusive enum moved; this pin can no longer find it");
        let body = &enum_src[start..];
        let body = &body[..body.find("\n}").expect("Inconclusive has no closing brace")];

        // Accumulate each variant's full declaration, so a multi-line struct variant
        // is judged as one unit rather than line by line.
        let mut variants: Vec<(String, String)> = Vec::new();
        let mut pending = String::new();
        for line in body.lines().skip(1) {
            let trimmed = line.trim();
            if trimmed.starts_with("///") || trimmed.starts_with("#[") || trimmed.is_empty() {
                continue;
            }
            pending.push_str(trimmed);
            pending.push(' ');
            // A declaration ends at the comma that closes it, once brackets balance.
            let balanced = pending.matches('(').count() == pending.matches(')').count()
                && pending.matches('{').count() == pending.matches('}').count();
            if !(trimmed.ends_with(',') && balanced) {
                continue;
            }
            let declaration = std::mem::take(&mut pending);
            let name: String = declaration
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                variants.push((name, declaration));
            }
        }

        let text_carrying: Vec<&str> = variants
            .iter()
            .filter(|(_, declaration)| {
                declaration.contains("String") || declaration.contains("str")
            })
            .map(|(name, _)| name.as_str())
            .collect();

        // Fail closed, on both halves. If the scrape silently matched nothing, or
        // matched only text-carrying variants, every assertion below would pass
        // vacuously and the pin would guard the wildcard by doing nothing at all.
        assert!(
            variants.len() >= 8,
            "the variant scrape broke: found {} declaration(s) in an enum that has \
             far more: {variants:?}",
            variants.len()
        );
        assert!(
            text_carrying.len() >= 3,
            "expected at least the known text-carrying variants (ContractError, \
             ResourceLimit, MalformedCase); the scrape found {text_carrying:?}"
        );
        assert!(
            variants.len() > text_carrying.len(),
            "every variant was judged text-carrying, so the filter is matching \
             something other than what it claims: {variants:?}"
        );

        let detail_src = include_str!("conformance.rs");
        let fn_start = detail_src
            .find("fn inconclusive_detail(")
            .expect("inconclusive_detail moved; this pin can no longer find it");
        let fn_body = &detail_src[fn_start..];
        let fn_body = &fn_body[..fn_body.find("\n}").expect("inconclusive_detail has no end")];

        for variant in text_carrying {
            assert!(
                fn_body.contains(&format!("Inconclusive::{variant}")),
                "Inconclusive::{variant} carries text but inconclusive_detail does \
                 not report it, so it would vanish into the wildcard arm and be \
                 undiagnosable from the tool's output - the exact defect #5461 fixed"
            );
        }
    }

    /// The whole point of the channel: a code diagnostic is DIAGNOSTIC. It must
    /// not be counted as a violation, and it must not fail the command — a
    /// deprecation notice that broke every affected author's build in release
    /// *n* would make the two-release notice period meaningless.
    #[test]
    fn a_code_diagnostic_is_never_removal_eligible() {
        let outcomes = vec![(
            ConformanceCase::new(
                ConformanceProperty::StateCommutativity,
                vec![Bytes::from(vec![1u8]), Bytes::from(vec![2u8])],
            ),
            PropertyOutcome::Holds,
        )];
        let report = Report::build(
            &Corpus::default(),
            &outcomes,
            None,
            vec![CodeDiagnostic {
                diagnostic: "host_clock_import",
                detail: "reads the clock".to_string(),
            }],
        );
        assert_eq!(
            (report.violations, report.enforceable_violations),
            (0, 0),
            "a code diagnostic was counted as a merge-law violation; it is about \
             the contract's code, no law was checked, and counting it here makes \
             it removal-eligible"
        );
        assert_eq!(report.code_diagnostics.len(), 1);
        // `exit_code` reads outcomes, never the report, so no code diagnostic can
        // reach it however it is worded.
        let just_outcomes: Vec<&PropertyOutcome> = outcomes.iter().map(|(_, o)| o).collect();
        assert_eq!(exit_code(just_outcomes), 0);
    }

    /// The diagnostic has to reach a human who did not ask for `--json`, and it
    /// has to reach one who did.
    #[test]
    fn the_human_report_shows_a_code_diagnostic() {
        let report = report_with_code_diagnostics(vec![CodeDiagnostic {
            diagnostic: "host_clock_import",
            detail: "this contract imports the host wall clock".to_string(),
        }]);

        let rendered = render_human(&report);
        assert!(
            rendered.contains("host_clock_import")
                && rendered.contains("this contract imports the host wall clock"),
            "the code diagnostic never reached the default (non-json) output:\n{rendered}"
        );
        assert!(
            rendered.contains("code diagnostic(s) above"),
            "'no enforceable violations found.' is the last thing this report \
             says, so a contract whose clock call will trap in a future release \
             reads as a clean bill of health:\n{rendered}"
        );

        let json = serde_json::to_string(&report).expect("the report serializes");
        assert!(
            json.contains("host_clock_import"),
            "--json dropped the code diagnostic, so no automation can see it:\n{json}"
        );
    }

    /// And a contract with nothing to say must not be told it has something to
    /// say. Without this the pin above is satisfied by printing the section
    /// unconditionally.
    #[test]
    fn a_report_with_no_code_diagnostics_prints_no_section() {
        let rendered = render_human(&report_with_code_diagnostics(Vec::new()));
        assert!(
            !rendered.contains("code diagnostic"),
            "a clean run was told about code diagnostics it does not have:\n{rendered}"
        );
    }

    /// The standalone rendering used by the paths that abort before a `Report`
    /// exists carries the diagnostic's own detail, not just its name — that
    /// detail is the whole message (what is wrong, what to do instead, and the
    /// docs link).
    #[test]
    fn the_standalone_rendering_carries_the_detail() {
        let wasm = module_importing(&[(
            host_clock::HOST_CLOCK_NAMESPACE,
            host_clock::HOST_CLOCK_IMPORT,
        )]);
        let rendered = render_code_diagnostics_standalone(&code_diagnostics(&wasm))
            .expect("a clock-importing contract must render a standalone diagnostic");
        assert!(
            rendered.contains("host_clock_import"),
            "the standalone rendering omits the diagnostic's machine-readable \
             name:\n{rendered}"
        );
        assert!(
            rendered.contains(freenet::conformance::HOST_CLOCK_DEPRECATION_DOC),
            "the standalone rendering omits the docs link, which is the only part \
             that tells an author what to do:\n{rendered}"
        );
    }

    /// And says nothing when there is nothing to say, so a clean contract's
    /// aborted run does not grow a spurious section.
    #[test]
    fn the_standalone_rendering_is_silent_when_there_is_nothing_to_say() {
        assert_eq!(render_code_diagnostics_standalone(&[]), None);
    }

    /// `fdev verify-merge --wasm mycontract.wasm` — the plain invocation with no
    /// corpus — reaches the bail that the diagnostic must be reported before.
    ///
    /// This is the exact case the reachability work was written for and the one
    /// it originally missed: `load_inputs` bails on a missing corpus, which
    /// short-circuits `load_inputs(&config)?` in `conformance()` and skips the
    /// guards there entirely. This test fixes the invocation and establishes
    /// both halves of the problem — that it bails, and that the contract it
    /// bailed on genuinely has something to report. The ordering itself is
    /// pinned by `the_no_corpus_bail_reports_diagnostics_first`.
    #[test]
    fn the_plain_wasm_only_invocation_bails_on_a_contract_that_has_something_to_report() {
        use clap::Parser;

        let wasm = module_importing(&[(
            host_clock::HOST_CLOCK_NAMESPACE,
            host_clock::HOST_CLOCK_IMPORT,
        )]);
        assert!(
            !code_diagnostics(&wasm).is_empty(),
            "the fixture must have something to report, or this test proves nothing"
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("mycontract.wasm");
        std::fs::write(&path, &wasm).expect("write fixture");

        let config = ConformanceConfig::parse_from([
            "verify-merge",
            "--wasm",
            path.to_str().expect("utf-8 temp path"),
        ]);
        let err = load_inputs(&config)
            .expect_err("--wasm with no --state must not silently succeed")
            .to_string();
        assert!(
            err.contains("at least one --state or --transition is required"),
            "this test is no longer exercising the no-corpus bail: {err}"
        );
    }
}

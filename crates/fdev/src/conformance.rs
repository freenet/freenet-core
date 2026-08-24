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

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Context;
use freenet::conformance::ConformanceOracle;
use freenet::conformance::generator::Corpus;
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
            for r in &self.inconclusive_reasons {
                writeln!(out, "  {}: {}", r.reason, r.occurrences)?;
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

        if self.enforceable_violations == 0 {
            writeln!(out, "\nno enforceable violations found.")?;
            if self.diagnostic_violations > 0 {
                writeln!(
                    out,
                    "({} diagnostic finding(s) above are efficiency notes, not merge-law breaks, and do not fail this command.)",
                    self.diagnostic_violations
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
        panic!("{signature}'s body is not brace-balanced");
    }

    /// A function's body with whole-line comments stripped, so a comment mentioning
    /// the searched-for token can neither satisfy nor defeat the assertion. The
    /// comment above the `eprintln!` call names both macros; the comment above the
    /// report's no-evidence blocks named both fields the old pin looked for.
    ///
    /// Takes the signature rather than hard-coding one, so that the comment-stripping
    /// version is the ONLY way to scrape a body in this file.
    fn code_only(signature: &str) -> String {
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
}

//! The verifier against real WASM, not the pure-Rust fakes.
//!
//! `tests.rs` proves the property *logic* is right: each check fires on exactly
//! the law it claims to, against contracts made of plain Rust closures. That is
//! necessary but not sufficient — it never touches [`RuntimeOracle`], the wasmtime
//! path, buffer streaming, or the classification of a real WASM trap. This file
//! is the other half: the same defects, planted in a real compiled contract
//! (`tests/test-contract-conformance`), driven through [`RuntimeOracle::standalone`].
//!
//! Compiling WASM is slow, so this is one test, not several: it compiles the
//! shared module once and drives one [`RuntimeOracle`] per mode against it.

use std::sync::Arc;

use super::generator::{Corpus, GeneratorConfig, generate_cases};
use super::property::{ConformanceProperty, PropertyOutcome};
use super::runtime_oracle::RuntimeOracle;
use super::verifier::{Bytes, ConformanceCase, verify_case};

// Keep in sync with the mode constants in `tests/test-contract-conformance/src/lib.rs`.
const CONFORMING: u8 = 0;
const LAST_WRITE_WINS: u8 = 1;
const MUTUAL_REJECTION: u8 = 2;
const NON_IDEMPOTENT_DELTA: u8 = 3;
const NONDETERMINISTIC_SUMMARY: u8 = 4;
const CAPPED_SET: u8 = 5;
const NEVER_SETTLES: u8 = 6;

fn bytes(values: &[u8]) -> Bytes {
    Arc::from(values)
}

#[track_caller]
fn assert_violates(outcome: PropertyOutcome, property: ConformanceProperty) {
    match outcome {
        PropertyOutcome::Violated(v) => assert_eq!(v.property, property, "wrong property flagged"),
        other @ (PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_)) => {
            panic!("expected a {property} violation, got {other:?}")
        }
    }
}

/// A module that compiles but is not a contract must fail at LOAD, not later as an
/// inconclusive check result.
///
/// `Runtime::compile_check` exists solely to draw that line, and until this test it
/// had no coverage of its own: every other test loads the fixture, which exports the
/// whole ABI, so `missing_contract_exports_for` returned an empty list on every run
/// and deleting the check entirely would not have failed anything.
///
/// The distinction is load-bearing for the rest of the module. "Could not load this
/// contract" is a hard error the caller must see; "could not judge this contract" is
/// a routine, benign outcome that must never be treated as a finding. Collapsing the
/// first into the second would report a broken or non-contract WASM as an ordinary
/// inconclusive result, which reads as "nothing to worry about here".
///
/// The input is the eight-byte WASM header and nothing else: a valid, empty module,
/// which is the cheapest way to be a module that exports none of what a contract
/// must export.
#[tokio::test(flavor = "multi_thread")]
async fn a_module_that_is_not_a_contract_fails_to_load_rather_than_reading_inconclusive() {
    // Exports a memory (so it gets past instantiation, which needs one) and none of
    // the contract entry points, which is what leaves `missing_contract_exports_for`
    // something to find. A bare eight-byte header would be rejected earlier, for
    // lacking the memory export, and would never reach the ABI check at all.
    let not_a_contract = br#"(module (memory (export "memory") 1))"#.to_vec();

    let err = RuntimeOracle::standalone(not_a_contract, vec![])
        .await
        .err()
        .expect("a module exporting no contract entry points must not load");

    let text = err.to_string();
    assert!(
        text.contains("not a contract") || text.contains("missing required export"),
        "the error must say the module is not a contract, so a caller cannot mistake \
         it for a contract that merely could not be judged; got: {text}"
    );
}

/// Proves the whole pipeline against real wasmtime: the verifier's property
/// logic, [`RuntimeOracle`]'s translation of contract calls, and the streaming
/// buffer protocol all have to agree for these outcomes to come out right.
#[tokio::test(flavor = "multi_thread")]
async fn verifier_matches_real_wasm_for_every_planted_defect()
-> Result<(), Box<dyn std::error::Error>> {
    let wasm = crate::wasm_runtime::tests::get_test_module("test_contract_conformance")?;

    // ------------------------------------------------------- mode 0: conforming
    //
    // The most important assertion in this file. A genuine join-semilattice run
    // through real wasmtime must never be flagged, across every property and a
    // non-trivial number of generated cases — the false-positive gate against
    // real WASM execution, not just against the pure-Rust fakes in `tests.rs`.
    let mut conforming = RuntimeOracle::standalone(wasm.clone(), vec![CONFORMING]).await?;
    let corpus = Corpus {
        deltas: vec![bytes(&[9]), bytes(&[7])],
        ..Corpus::from_states(vec![
            vec![1],
            vec![2],
            vec![1, 2],
            vec![2, 3],
            vec![1, 2, 3],
            vec![4, 5],
        ])
    };
    let cases = generate_cases(
        &corpus,
        &GeneratorConfig {
            max_cases: 60,
            ..Default::default()
        },
    );
    assert!(
        cases.len() >= ConformanceProperty::ALL.len(),
        "checked too few cases through real WASM to mean anything: {}",
        cases.len()
    );
    for case in &cases {
        let outcome = verify_case(&mut conforming, case);
        assert!(
            !outcome.is_violation(),
            "false positive on a conforming contract through real WASM: {outcome:?} for {}",
            case.property
        );
    }

    // ------------------------------------------------- mode 1: last-write-wins
    let mut lww = RuntimeOracle::standalone(wasm.clone(), vec![LAST_WRITE_WINS]).await?;
    let commutativity_case = ConformanceCase::new(
        ConformanceProperty::StateCommutativity,
        vec![bytes(&[1, 2]), bytes(&[2, 3])],
    );
    assert_violates(
        verify_case(&mut lww, &commutativity_case),
        ConformanceProperty::StateCommutativity,
    );

    // ------------------------------------------------- mode 2: mutual rejection
    let mut rejection = RuntimeOracle::standalone(wasm.clone(), vec![MUTUAL_REJECTION]).await?;
    assert_violates(
        verify_case(&mut rejection, &commutativity_case),
        ConformanceProperty::StateCommutativity,
    );
    let cycle_case = ConformanceCase::new(
        ConformanceProperty::ReconciliationCycle,
        vec![bytes(&[1, 2]), bytes(&[3, 4])],
    );
    assert_violates(
        verify_case(&mut rejection, &cycle_case),
        ConformanceProperty::ReconciliationCycle,
    );

    // --------------------------------------------- mode 3: non-idempotent delta
    let mut non_idempotent =
        RuntimeOracle::standalone(wasm.clone(), vec![NON_IDEMPOTENT_DELTA]).await?;
    // [9] genuinely applies against [1, 2] (it isn't already in the set), so the
    // first application changes the state and idempotence has something to fail.
    let idempotence_case =
        ConformanceCase::new(ConformanceProperty::DeltaIdempotence, vec![bytes(&[1, 2])])
            .with_deltas(vec![bytes(&[9])]);
    assert_violates(
        verify_case(&mut non_idempotent, &idempotence_case),
        ConformanceProperty::DeltaIdempotence,
    );

    // ------------------------------------------ mode 4: nondeterministic summary
    let mut nondeterministic =
        RuntimeOracle::standalone(wasm.clone(), vec![NONDETERMINISTIC_SUMMARY]).await?;
    let summary_case = ConformanceCase::new(
        ConformanceProperty::SummaryDeterminism,
        vec![bytes(&[1, 2])],
    );
    assert_violates(
        verify_case(&mut nondeterministic, &summary_case),
        ConformanceProperty::SummaryDeterminism,
    );

    // ------------------------------------------------- mode 5: capped collection
    //
    // The only planted defect that the pairwise laws cannot see. Asserting the
    // associativity violation alone would not show that: a mode that also broke
    // commutativity would satisfy that assertion while proving nothing about the
    // three-state check. So assert the pairwise laws still HOLD here — that is
    // what makes this a test of associativity specifically.
    let mut capped = RuntimeOracle::standalone(wasm.clone(), vec![CAPPED_SET]).await?;
    let associativity_case = ConformanceCase::new(
        ConformanceProperty::StateAssociativity,
        vec![bytes(&[1, 2]), bytes(&[3, 4]), bytes(&[5, 6])],
    );
    assert_violates(
        verify_case(&mut capped, &associativity_case),
        ConformanceProperty::StateAssociativity,
    );
    for pairwise in [
        ConformanceCase::new(
            ConformanceProperty::StateCommutativity,
            vec![bytes(&[1, 2]), bytes(&[3, 4])],
        ),
        ConformanceCase::new(
            ConformanceProperty::StateIdempotence,
            vec![bytes(&[1, 2]), bytes(&[3, 4])],
        ),
    ] {
        let outcome = verify_case(&mut capped, &pairwise);
        assert!(
            !outcome.is_violation(),
            "the capped-collection mode should break associativity ALONE, but {} \
             also reported a violation: {outcome:?}",
            pairwise.property
        );
    }

    // ------------------------------------------------ mode 6: never settles
    //
    // Regression cover for the worst shape found on the live network: a contract
    // whose merge rewrote its state on every apply, so `merge(A, A)` never reached a
    // fixpoint. Under at-least-once delivery such a contract can never converge, and
    // it is covered here by shape rather than by committing the third-party WASM it
    // was observed in.
    let mut never_settles = RuntimeOracle::standalone(wasm.clone(), vec![NEVER_SETTLES]).await?;
    assert_violates(
        verify_case(
            &mut never_settles,
            &ConformanceCase::new(ConformanceProperty::StateIdempotence, vec![bytes(&[1, 2])]),
        ),
        ConformanceProperty::StateIdempotence,
    );

    // ---------------------------------- same code, different params, different instance
    let a = RuntimeOracle::standalone(wasm.clone(), vec![CONFORMING]).await?;
    let b = RuntimeOracle::standalone(wasm, vec![LAST_WRITE_WINS]).await?;
    assert_ne!(
        a.instance_id(),
        b.instance_id(),
        "same code with different parameters must be a different instance"
    );

    Ok(())
}

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

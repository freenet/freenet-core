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
const REQUIRES_RELATED: u8 = 7;
const PATH_DISAGREEMENT: u8 = 8;
const RELATED_ID: [u8; 32] = [7; 32];

fn bytes(values: &[u8]) -> Bytes {
    Arc::from(values)
}

/// Build the `(base, result)` step a peer would have recorded, by driving the
/// contract's own delta path through the real runtime.
///
/// Computed rather than hard-coded so the case cannot drift into asserting against
/// a result this contract would never actually produce.
fn transition_case(
    oracle: &mut RuntimeOracle,
    base: &[u8],
    delta: &[u8],
) -> Result<ConformanceCase, Box<dyn std::error::Error>> {
    use super::oracle::ConformanceOracle;
    let result = oracle
        .update_state(
            base,
            &[freenet_stdlib::prelude::UpdateData::Delta(
                freenet_stdlib::prelude::StateDelta::from(delta.to_vec()),
            )],
        )?
        .new_state
        .ok_or("contract produced no state")?
        .into_bytes();
    assert_ne!(
        base,
        result.as_slice(),
        "a transition that changed nothing proves nothing"
    );
    Ok(ConformanceCase::new(
        ConformanceProperty::TransitionPathAgreement,
        vec![bytes(base), bytes(&result)],
    ))
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
    // The only planted defect that the PAIRWISE state laws cannot see. Asserting the
    // associativity violation alone would not show that: a mode that also broke
    // commutativity would satisfy that assertion while proving nothing about the
    // three-state check. So assert the pairwise laws still HOLD here — that is
    // what makes this a test of associativity specifically.
    //
    // "Pairwise" is doing real work in that sentence: the transition law sees this
    // mode too, from the same information loss, and that is asserted deliberately
    // further down rather than being an exception this loop must dodge. The claim
    // is that associativity is the law that NAMES the defect and no pairwise state
    // law fires, not that nothing else in the module can see it.
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
        let property = pairwise.property;
        assert_eq!(
            verify_case(&mut capped, &pairwise),
            PropertyOutcome::Holds,
            "among the PAIRWISE state laws the capped-collection mode should break \
             none — associativity is the one that names it — but {property} did not \
             hold"
        );
    }

    // The transition law and a bounded collection.
    //
    // This is the false-positive boundary the `transition_path_agreement` severity
    // rests on, so it is measured here against real WASM rather than argued. This
    // cap evicts by an index derived from the set's own contents — independent of
    // the merge's ordering — and merging the reached state back into its base
    // genuinely does not reproduce it. That is the same information loss
    // `StateAssociativity` already reports for this mode, so the contract is
    // removal-eligible either way; what matters is that it is a real divergence and
    // not an artifact of capping.
    //
    // The cap applies to the mode's DELTA path as well as its merge path, which is
    // what makes that true. Without it the recorded `result` would carry more than
    // CAP entries — a state the merge path can never emit — and the assertion below
    // would land on the right verdict for the wrong reason: it would be reporting
    // the missing cap, not the eviction rule.
    //
    // The contrasting SOUND cap (keep the largest N, evicting BY the merge order) is
    // pinned silent in `tests.rs::a_sound_bounded_collection_is_not_accused` — the
    // two together are what show the property discriminates rather than flagging
    // every bounded collection.
    let capped_transition = transition_case(&mut capped, &[1, 2], &[3, 4, 5])?;
    // The recorded result must be a state the MERGE path could also have emitted.
    //
    // Without the cap on the delta path this is a five-entry state against a cap of
    // three — something `merge_state` can never produce — and the violation below
    // would be reporting the missing cap rather than the eviction rule. The
    // assertion would land on the right verdict for the wrong reason, which is the
    // failure shape this whole module is built to avoid.
    assert!(
        capped_transition.states[1].len() <= 3,
        "the delta path must respect the cap, or this case is about a state the \
         merge path can never reach: {:?}",
        capped_transition.states[1]
    );
    assert_violates(
        verify_case(&mut capped, &capped_transition),
        ConformanceProperty::TransitionPathAgreement,
    );

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
    // Associativity breaks too, which the mode's documentation claims and this
    // asserts rather than leaving in prose. Needs three states by definition.
    assert_violates(
        verify_case(
            &mut never_settles,
            &ConformanceCase::new(
                ConformanceProperty::StateAssociativity,
                vec![bytes(&[1, 2]), bytes(&[2, 3]), bytes(&[5, 6])],
            ),
        ),
        ConformanceProperty::StateAssociativity,
    );
    // Commutativity still HOLDS for this arm: the rewrite is applied to a union, and
    // a union is symmetric. Asserting it keeps the mode's description honest rather
    // than leaving a reader to redo the algebra — the same self-check the
    // capped-collection mode carries, and the reason review caught the doc claiming
    // otherwise.
    let commutativity_still_holds = verify_case(
        &mut never_settles,
        &ConformanceCase::new(
            ConformanceProperty::StateCommutativity,
            vec![bytes(&[1, 2]), bytes(&[2, 3])],
        ),
    );
    assert!(
        !commutativity_still_holds.is_violation(),
        "this mode should break idempotence and associativity only; a commutativity \
         finding means the arm no longer isolates what its documentation claims: \
         {commutativity_still_holds:?}"
    );

    // ------------------------------------------- mode 7: needs related state
    //
    // Why capturing related-contract state matters: without it the verifier reaches
    // no verdict at all for this class of contract, which is honest and useless.
    // Measured against a live capture, three of 32 contracts were in exactly this
    // position, one across sixty cases.
    let mut needs_related = RuntimeOracle::standalone(wasm.clone(), vec![REQUIRES_RELATED]).await?;
    let case = ConformanceCase::new(
        ConformanceProperty::StateCommutativity,
        vec![bytes(&[1, 2]), bytes(&[2, 3])],
    );
    match verify_case(&mut needs_related, &case) {
        PropertyOutcome::Inconclusive(reason) => assert_eq!(
            reason,
            crate::conformance::property::Inconclusive::RelatedRequired,
            "a contract asking for related state must be declined, never accused"
        ),
        other @ (PropertyOutcome::Holds | PropertyOutcome::Violated(_)) => {
            panic!("expected RelatedRequired without the related state, got {other:?}")
        }
    }

    // Supply it and the same case decides. The merge conforms, so the verdict is
    // Holds; the point is that there is a verdict at all.
    let mut supplied = std::collections::HashMap::new();
    supplied.insert(
        freenet_stdlib::prelude::ContractInstanceId::new(RELATED_ID),
        Some(freenet_stdlib::prelude::State::from(vec![1u8, 2])),
    );
    let with_related = ConformanceCase::new(
        ConformanceProperty::StateCommutativity,
        vec![bytes(&[1, 2]), bytes(&[2, 3])],
    )
    .with_related(freenet_stdlib::prelude::RelatedContracts::from(supplied));
    assert_eq!(
        verify_case(&mut needs_related, &with_related),
        PropertyOutcome::Holds,
        "with the related state supplied the contract must become judgeable"
    );

    // ------------------------------------------- mode 8: disagreeing write paths
    //
    // The #5394 shape, and the one mode whose defect NO pre-existing property can
    // see: the delta path takes the last write on a key collision, the merge path
    // the first. Each rule is a sound semilattice on its own, so every law that
    // compares merge-to-merge or delta-to-delta holds.
    //
    // `0x51` and `0x52` are two writes to key 5 with different values — two ops
    // carrying the same client-chosen sequence number, which is exactly the
    // collision the real defect resolved two different ways.
    let mut disagreeing = RuntimeOracle::standalone(wasm.clone(), vec![PATH_DISAGREEMENT]).await?;
    let colliding: Vec<Bytes> = vec![bytes(&[0x10, 0x51]), bytes(&[0x10, 0x52])];
    assert_violates(
        verify_case(
            &mut disagreeing,
            &ConformanceCase::new(ConformanceProperty::PathAgreement, colliding.clone()),
        ),
        ConformanceProperty::PathAgreement,
    );

    // Every OTHER property must stay silent on the same inputs. Without this the
    // mode would prove nothing about the gap #5394 describes, which is precisely a
    // contract that satisfies the entire existing property set and still diverges.
    for property in ConformanceProperty::ALL {
        // Both halves of the path-agreement family see this defect, which is the
        // point of the mode; they are asserted positively above and below instead.
        if matches!(
            property,
            ConformanceProperty::PathAgreement | ConformanceProperty::TransitionPathAgreement
        ) {
            continue;
        }
        let states: Vec<Bytes> = match property.state_arity() {
            3 => vec![
                colliding[0].clone(),
                colliding[1].clone(),
                bytes(&[0x23, 0x51]),
            ],
            _ => colliding.clone(),
        };
        let deltas: Vec<Bytes> = match property.delta_arity() {
            0 => Vec::new(),
            1 => vec![bytes(&[0x52])],
            _ => vec![bytes(&[0x52]), bytes(&[0x63])],
        };
        // `Holds`, not merely "not a violation": `Inconclusive` also satisfies
        // `!is_violation()`, and the claim this loop supports is that every other
        // law HOLDS on this mode. A case that stopped being evaluated at all would
        // keep the loop green while the #5394 gap argument quietly lost its
        // evidence.
        assert_eq!(
            verify_case(
                &mut disagreeing,
                &ConformanceCase::new(*property, states).with_deltas(deltas),
            ),
            PropertyOutcome::Holds,
            "{property} did not HOLD on the disagreeing-paths mode, so it no longer \
             isolates the one defect only path_agreement can see"
        );
    }

    // The transition-shaped half of the same law, on the same mode. This is the form
    // that reaches the #5394 artifacts, where the contract's own `get_state_delta`
    // ships whole states and the pairwise form is therefore blind.
    let disagreeing_transition = transition_case(&mut disagreeing, &[0x10, 0x51], &[0x52])?;
    assert_violates(
        verify_case(&mut disagreeing, &disagreeing_transition),
        ConformanceProperty::TransitionPathAgreement,
    );

    // Its matched negative: the same contract, an op whose key collides with nothing.
    let harmless_transition = transition_case(&mut disagreeing, &[0x10, 0x51], &[0x62])?;
    assert_eq!(
        verify_case(&mut disagreeing, &harmless_transition),
        PropertyOutcome::Holds,
        "a transition whose op collides with nothing must not be flagged, or the \
         property is a blanket accusation against every contract with a delta path"
    );

    // The matched negative #5394's acceptance test asks for: the SAME contract with
    // the SAME two write paths, on states whose keys do not collide. A property that
    // flagged every contract with both a delta and a merge path would pass the
    // assertion above while being worse than no property at all.
    assert_eq!(
        verify_case(
            &mut disagreeing,
            &ConformanceCase::new(
                ConformanceProperty::PathAgreement,
                vec![bytes(&[0x10, 0x51]), bytes(&[0x10, 0x62])],
            ),
        ),
        PropertyOutcome::Holds,
        "the two write paths only disagree on a key COLLISION; flagging a pair that \
         has none makes this a blanket accusation rather than a finding"
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

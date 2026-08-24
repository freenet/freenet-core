//! Tests for the sample store and focus selection.
//!
//! These are the deterministic tests the RFC asks for by name: hash deduplication,
//! oscillation not consuming budget, the strata, a seeded reservoir, overlap storing
//! bytes once, byte budgets, large states, restart reconstruction, and focus
//! rotation.

use std::sync::Arc;

use freenet_stdlib::prelude::ContractInstanceId;

use super::focus::FocusSelector;
use super::sampler::{Admission, ContractSampler, SamplerConfig, Stratum};

fn config() -> SamplerConfig {
    SamplerConfig {
        earliest: 2,
        recent: 3,
        reservoir: 3,
        largest: 2,
        smallest: 2,
        transitions: 3,
        max_bytes: 4096,
        max_state_bytes: 512,
        seed: 42,
    }
}

fn state(tag: u8, len: usize) -> Vec<u8> {
    let mut out = vec![tag];
    out.resize(len.max(1), tag);
    out
}

/// The node's own corpus must carry the ORDERED step, not just the two states.
///
/// `TransitionPathAgreement` is a law only because the corpus witnesses that one
/// state was reached from the other; for an arbitrary pair, "merging B into A yields
/// B" is last-write-wins and false for every conforming contract. So the generator
/// builds no case for it at all without provenance.
///
/// This pins the in-memory path specifically. `to_bundle` already exported
/// transitions, but `corpus()` — which is what shadow mode on a live node actually
/// checks — pulled only `delta` and `summary` out of them and dropped the pairing.
/// A property that ran on a hand-built `fdev --bundle` corpus and never once on the
/// network would be the most expensive kind of miss: it reads as coverage.
#[test]
fn the_in_memory_corpus_carries_transition_provenance() {
    let mut sampler = ContractSampler::new(config());
    let base = state(1, 32);
    let result = state(2, 48);
    assert_eq!(
        sampler.observe_transition(&base, None, Some(&[9]), None, &result),
        Admission::Stored
    );

    let corpus = sampler.corpus();
    assert_eq!(
        corpus.transitions.len(),
        1,
        "the ordered base -> result step is what the transition law needs, and \
         without it shadow mode checks that law on nothing"
    );
    assert_eq!(corpus.transitions[0].0.as_ref(), base.as_slice());
    assert_eq!(corpus.transitions[0].1.as_ref(), result.as_slice());

    // The exported bundle and the in-memory corpus must agree about what a
    // transition is, or a finding on a live node would vanish on replay.
    let bundle = sampler.to_bundle(None, Some([0u8; 32]), Vec::new());
    assert_eq!(bundle.transitions.len(), 1);
    let replayed = bundle.to_corpus();
    assert_eq!(replayed.transitions, corpus.transitions);

    // ...and about what a delta was applied TO. `delta_bases` is the second thing a
    // transition carries, and the only thing that pairs deltas for
    // `delta_permutation_invariance` — which pairs only deltas observed against the
    // SAME base. Comparing transitions alone lets an export drop every base while
    // this pin stays green, and a replay that quietly covers less than the run it
    // replays is the worst shape an export can have.
    assert_eq!(
        corpus.delta_bases,
        vec![Some(Arc::from(base.as_slice()))],
        "the observed delta must be recorded against the state it was applied to"
    );
    assert_eq!(replayed.delta_bases, corpus.delta_bases);
    assert_eq!(replayed.deltas, corpus.deltas);
}

// ------------------------------------------------------------------ deduplication

#[test]
fn identical_states_are_stored_once() {
    let mut sampler = ContractSampler::new(config());
    assert_eq!(sampler.observe_state(&state(1, 10)), Admission::Stored);
    assert_eq!(sampler.observe_state(&state(1, 10)), Admission::Duplicate);
    assert_eq!(sampler.distinct_states(), 1);
    assert_eq!(sampler.total_seen(), 2);
    assert_eq!(sampler.distinct_seen(), 1);
}

/// The contracts this mechanism exists to catch are the least diverse ones: a
/// contract stuck oscillating offers the same two states forever. If those two
/// consumed a sample slot each time, the store would be full of one bug's worth of
/// material and blind to everything else.
#[test]
fn oscillation_does_not_consume_sample_budget() {
    let mut sampler = ContractSampler::new(config());
    let (a, b) = (state(1, 32), state(2, 32));
    for _ in 0..500 {
        sampler.observe_state(&a);
        sampler.observe_state(&b);
    }
    assert_eq!(sampler.distinct_states(), 2);
    assert_eq!(sampler.distinct_seen(), 2);
    assert_eq!(sampler.total_seen(), 1000);
    assert!(sampler.stored_bytes() <= 64 + 8);
}

// ------------------------------------------------------------------------- strata

#[test]
fn earliest_keeps_the_first_states_and_recent_keeps_the_last() {
    let mut sampler = ContractSampler::new(config());
    for tag in 1u8..=8 {
        sampler.observe_state(&state(tag, 16));
    }

    let earliest = sampler.members(Stratum::Earliest).to_vec();
    assert_eq!(
        earliest.len(),
        2,
        "earliest is capped at its configured size"
    );

    // Recency must reflect the tail of the stream, not the head. Concretely: the
    // most recent state observed has to still be retrievable.
    let corpus = sampler.corpus();
    assert!(
        corpus.states.iter().any(|s| s.as_ref()[0] == 8),
        "the most recently observed state was not retained"
    );
}

#[test]
fn largest_and_smallest_track_size_not_arrival_order() {
    let mut sampler = ContractSampler::new(config());
    for (tag, len) in [(1u8, 400), (2, 8), (3, 200), (4, 4), (5, 100)] {
        sampler.observe_state(&state(tag, len));
    }
    let corpus = sampler.corpus();
    let sizes: Vec<usize> = corpus.states.iter().map(|s| s.len()).collect();
    assert!(
        sizes.contains(&400),
        "the largest observed state must be retained: {sizes:?}"
    );
    assert!(
        sizes.contains(&4),
        "the smallest observed state must be retained: {sizes:?}"
    );
}

/// A state can qualify for several strata at once. Its bytes must still be stored
/// once — otherwise the byte budget is measuring something other than memory.
#[test]
fn a_state_in_several_strata_is_stored_once() {
    let mut sampler = ContractSampler::new(config());
    // The very first state is simultaneously earliest, recent, reservoir, largest
    // and smallest, since it is the only one there is.
    sampler.observe_state(&state(1, 100));
    assert_eq!(sampler.distinct_states(), 1);
    assert_eq!(sampler.stored_bytes(), 100);
}

// ---------------------------------------------------------------------- reservoir

/// The reservoir must be a pure function of the seed and the observation sequence.
/// A reservoir that drew from an RNG would sample differently on every restart, and
/// two peers replaying the same corpus would disagree about what they had seen.
#[test]
fn the_reservoir_is_deterministic_for_a_given_seed() {
    let run = || {
        let mut sampler = ContractSampler::new(config());
        for tag in 1u8..=40 {
            sampler.observe_state(&state(tag, 16));
        }
        sampler.members(Stratum::Reservoir).to_vec()
    };
    assert_eq!(run(), run());
}

#[test]
fn a_different_seed_samples_differently() {
    let sample_with = |seed: u64| {
        let mut sampler = ContractSampler::new(SamplerConfig { seed, ..config() });
        for tag in 1u8..=40 {
            sampler.observe_state(&state(tag, 16));
        }
        sampler.members(Stratum::Reservoir).to_vec()
    };
    // Peers with different seeds explore different parts of the stream, which is
    // what makes network-wide coverage broader than any one peer's.
    assert_ne!(sample_with(1), sample_with(999));
}

// ------------------------------------------------------------------------ budgets

#[test]
fn a_state_larger_than_the_per_state_ceiling_is_refused() {
    let mut sampler = ContractSampler::new(config());
    assert_eq!(sampler.observe_state(&state(1, 5000)), Admission::TooLarge);
    assert_eq!(sampler.distinct_states(), 0);
    assert_eq!(sampler.stored_bytes(), 0);
}

/// The limit that matters is bytes, not items: a handful of very large states must
/// not be able to blow up memory while every item count still looks healthy.
#[test]
fn the_byte_budget_is_never_exceeded() {
    let mut sampler = ContractSampler::new(config());
    for tag in 1u8..=200 {
        sampler.observe_state(&state(tag, 500));
    }
    assert!(
        sampler.stored_bytes() <= config().max_bytes,
        "stored {} bytes over a {} budget",
        sampler.stored_bytes(),
        config().max_bytes
    );
    assert!(
        sampler.distinct_states() > 0,
        "budget enforcement emptied the store instead of bounding it"
    );
}

#[test]
fn the_store_stays_useful_under_a_flood_of_large_states() {
    let mut sampler = ContractSampler::new(config());
    for tag in 1u8..=100 {
        sampler.observe_state(&state(tag, 500));
    }
    let corpus = sampler.corpus();
    assert!(
        corpus.states.len() >= 2,
        "a flood of large states left too little to check anything with"
    );
}

// --------------------------------------------------------------------- transitions

#[test]
fn transitions_carry_replay_context() {
    let mut sampler = ContractSampler::new(config());
    let base = state(1, 16);
    let result = state(2, 16);
    assert_eq!(
        sampler.observe_transition(&base, None, Some(&[9, 9]), Some(&[1]), &result),
        Admission::Stored
    );

    let corpus = sampler.corpus();
    assert!(corpus.deltas.iter().any(|d| d.as_ref() == [9, 9]));

    let bundle = sampler.to_bundle(None, Some([7u8; 32]), vec![]);
    assert_eq!(bundle.transitions.len(), 1);
    assert_eq!(bundle.transitions[0].base_state, base);
    assert_eq!(bundle.transitions[0].result_state, result);
}

#[test]
fn transitions_are_bounded() {
    let mut sampler = ContractSampler::new(config());
    for tag in 1u8..=20 {
        sampler.observe_transition(
            &state(tag, 16),
            None,
            Some(&[tag]),
            None,
            &state(tag + 1, 16),
        );
    }
    let bundle = sampler.to_bundle(None, Some([7u8; 32]), vec![]);
    assert!(bundle.transitions.len() <= config().transitions);
}

/// A transition pointing at bytes the store refused is not replayable, and keeping
/// it would leave a dangling reference that looks like usable evidence.
///
/// The refusal reports its own cause: this endpoint is over the per-state ceiling,
/// so the answer is `TooLarge` rather than a generic `NotSelected`. A caller
/// counting refusals needs to tell "too big to sample at all" apart from "the
/// strata were full", because only the first is worth acting on.
#[test]
fn a_transition_whose_endpoints_were_refused_is_not_recorded() {
    let mut sampler = ContractSampler::new(config());
    let huge = state(1, 5000);
    assert_eq!(
        sampler.observe_transition(&huge, None, None, None, &state(2, 16)),
        Admission::TooLarge
    );
    let bundle = sampler.to_bundle(None, Some([7u8; 32]), vec![]);
    assert!(bundle.transitions.is_empty());
}

/// Regression: transition payloads are contract-controlled and stored inline, and
/// were once left out of `stored_bytes()` entirely. A contract emitting large deltas
/// could then hold far more than the advertised ceiling while every count looked
/// healthy. A budget that measures only part of what it retains is not a budget.
#[test]
fn transition_payloads_count_against_the_byte_budget() {
    // Deliberately generous transition count and payloads, so the uncounted bytes
    // dominate: under the old accounting these were retained for free.
    let cfg = SamplerConfig {
        transitions: 24,
        max_bytes: 4096,
        ..config()
    };
    let mut sampler = ContractSampler::new(cfg.clone());
    // Each payload pair is 400 bytes: under the per-item ceiling (so it is admitted),
    // but 24 of them are 9600 bytes, well over the 4096 budget. If payloads are not
    // charged, they are retained for free.
    let payload = vec![7u8; 200];
    let mut admitted = 0;
    for tag in 1u8..=60 {
        if sampler.observe_transition(
            &state(tag, 16),
            None,
            Some(&payload),
            Some(&payload),
            &state(tag + 1, 16),
        ) == Admission::Stored
        {
            admitted += 1;
        }
    }
    assert!(
        admitted > cfg.transitions,
        "fixture failed: only {admitted} transitions were admitted, so nothing \
         accumulated and the assertions below prove nothing"
    );

    // Measured INDEPENDENTLY of `stored_bytes()`. Asserting on `stored_bytes()`
    // alone would be circular: that function is precisely what was wrong, so the
    // assertion would have passed against the buggy version. The serialized size is
    // an outside view of everything the store is actually holding.
    let actually_held = bincode::serialize(&sampler).expect("serialize").len();
    assert!(
        actually_held <= cfg.max_bytes + cfg.max_bytes / 2,
        "the store really holds {actually_held} bytes against a {} byte budget; \
         transition payloads are escaping the accounting",
        cfg.max_bytes
    );
    assert!(
        sampler.stored_bytes() <= cfg.max_bytes,
        "reported {} bytes over a {} budget",
        sampler.stored_bytes(),
        cfg.max_bytes
    );
}

/// Regression: the store could wedge permanently and stop learning, silently.
///
/// `earliest`, `largest` and `smallest` were exempt from eviction. Under the shape
/// of the production defaults — a per-state ceiling that is a large fraction of the
/// total budget — a contract whose first few observed states are big fills those
/// strata with unevictable blobs, and every later admission fails forever, because
/// the only evictable pools hold nothing but duplicate references to the same
/// pinned bytes. No error, no signal, and it happens to exactly the large-state
/// contracts this mechanism exists to examine.
///
/// The existing tests missed it because their config makes
/// `earliest + largest + smallest` incapable of reaching `max_bytes`.
#[test]
fn a_run_of_large_early_states_does_not_wedge_the_store() {
    // Deliberately production-shaped: one state can be a quarter of the budget.
    let cfg = SamplerConfig {
        earliest: 4,
        recent: 8,
        reservoir: 8,
        largest: 4,
        smallest: 4,
        transitions: 8,
        max_bytes: 4096,
        max_state_bytes: 1024,
        seed: 7,
    };
    let mut sampler = ContractSampler::new(cfg.clone());

    // Big states first, which is what fills the unevictable strata.
    for tag in 1u8..=8 {
        sampler.observe_state(&state(tag, 1000));
    }
    // Then small ones, which must still be admitted.
    let mut admitted_after = 0;
    for tag in 100u8..=140 {
        if sampler.observe_state(&state(tag, 8)) == Admission::Stored {
            admitted_after += 1;
        }
    }

    assert!(
        admitted_after > 0,
        "the store wedged: {} large states filled it and nothing could be admitted \
         afterwards, so it stops learning for the life of the contract",
        8
    );
    assert!(
        sampler.stored_bytes() <= cfg.max_bytes,
        "budget exceeded while unwedging: {} > {}",
        sampler.stored_bytes(),
        cfg.max_bytes
    );
    // The most recent observation must be reachable, which is the practical test of
    // "still learning" rather than merely "accepted something".
    let corpus = sampler.corpus();
    assert!(
        corpus.states.iter().any(|s| s.as_ref()[0] == 140),
        "the newest observation never made it into the corpus"
    );
}

#[test]
fn an_oversized_transition_payload_is_refused() {
    let mut sampler = ContractSampler::new(config());
    let huge = vec![7u8; config().max_state_bytes + 1];
    assert_eq!(
        sampler.observe_transition(&state(1, 16), None, Some(&huge), None, &state(2, 16)),
        Admission::TooLarge
    );
}

/// Regression: `recent` was a `VecDeque` read back via `as_slices().0`, which after
/// enough push_back/pop_front cycles returns only PART of the deque — so the newest
/// observations could silently vanish from the corpus. The comment at the time
/// claimed "nothing rotates it", which was exactly wrong: push_back plus pop_front
/// is rotation.
#[test]
fn the_recent_stratum_survives_many_rotations() {
    let mut sampler = ContractSampler::new(config());
    for tag in 1u8..=200 {
        sampler.observe_state(&state(tag, 16));
    }
    let recent = sampler.members(Stratum::Recent);
    assert_eq!(
        recent.len(),
        config().recent,
        "recent stratum lost members to deque slicing"
    );
    // The most recently observed state must be present, in the corpus as well as in
    // the stratum — that is the whole point of a "recent" stratum.
    let corpus = sampler.corpus();
    assert!(
        corpus.states.iter().any(|s| s.as_ref()[0] == 200),
        "the newest observation is missing from the corpus"
    );
}

/// Regression: re-observing a state that had aged out of `recent` (but was still
/// retained by another stratum) took the duplicate path and did nothing, so "recent"
/// stopped meaning "most recently observed" for exactly the states a peer keeps
/// seeing — the population that matters most.
#[test]
fn reobserving_an_aged_out_state_makes_it_recent_again() {
    let mut sampler = ContractSampler::new(config());
    let first = state(1, 16);
    sampler.observe_state(&first);
    // Push it out of the recent window (which holds `config().recent` entries).
    for tag in 2u8..=10 {
        sampler.observe_state(&state(tag, 16));
    }
    let hash = *blake3::hash(&first).as_bytes();
    assert!(
        !sampler.members(Stratum::Recent).contains(&hash),
        "fixture failed: the state never aged out, so the assertion below is vacuous"
    );

    assert_eq!(sampler.observe_state(&first), Admission::Duplicate);
    assert!(
        sampler.members(Stratum::Recent).contains(&hash),
        "a re-observed state did not return to the recent stratum"
    );
    assert!(
        sampler.members(Stratum::Recent).len() <= config().recent,
        "reinsertion broke the recent cap"
    );
}

// ------------------------------------------------------------------------ restart

/// A useful sample accumulates over hours. Losing it on restart would mean a peer
/// that reboots nightly never accumulates enough diversity to find anything.
#[test]
fn the_store_survives_a_restart() {
    let mut sampler = ContractSampler::new(config());
    for tag in 1u8..=12 {
        sampler.observe_state(&state(tag, 24));
    }
    sampler.observe_transition(&state(1, 24), None, Some(&[7]), None, &state(2, 24));

    let encoded = bincode::serialize(&sampler).expect("serialize");
    let restored: ContractSampler = bincode::deserialize(&encoded).expect("deserialize");

    assert_eq!(restored, sampler);
    assert_eq!(restored.distinct_seen(), sampler.distinct_seen());

    // And it must resume the same sampling sequence rather than restarting it: a
    // reservoir that reseeded on boot would bias toward whatever happened after the
    // last reboot.
    let mut a = sampler;
    let mut b = restored;
    for tag in 13u8..=30 {
        a.observe_state(&state(tag, 24));
        b.observe_state(&state(tag, 24));
    }
    assert_eq!(a.members(Stratum::Reservoir), b.members(Stratum::Reservoir));
}

// -------------------------------------------------------------------------- focus

fn instance(seed: u8) -> ContractInstanceId {
    ContractInstanceId::new([seed; 32])
}

#[test]
fn focus_is_bounded_and_stable_within_a_period() {
    let selector = FocusSelector::new([7; 32], 2);
    let candidates: Vec<_> = (1u8..=20).map(instance).collect();
    let first = selector.select(&candidates);
    assert_eq!(first.len(), 2);
    assert_eq!(
        first,
        selector.select(&candidates),
        "focus churned mid-period"
    );
}

#[test]
fn rotation_redraws_the_focus_set() {
    let mut selector = FocusSelector::new([7; 32], 2);
    let candidates: Vec<_> = (1u8..=20).map(instance).collect();
    let before = selector.select(&candidates);
    let mut changed = false;
    for _ in 0..8 {
        selector.rotate();
        if selector.select(&candidates) != before {
            changed = true;
            break;
        }
    }
    assert!(changed, "rotation never changed the focus set");
}

/// The load-bearing property: a contract author cannot predict or influence whether
/// a given peer is watching them, because selection is keyed on that peer's secret
/// salt. If focus were a function of the contract id alone, an author could grind
/// ids to be permanently unwatched — or to be watched by everyone, turning the
/// checking cost into an amplification vector.
#[test]
fn peers_with_different_salts_watch_different_contracts() {
    let candidates: Vec<_> = (1u8..=40).map(instance).collect();
    let mut distinct = std::collections::HashSet::new();
    for salt in 0u8..12 {
        distinct.insert(FocusSelector::new([salt; 32], 2).select(&candidates));
    }
    assert!(
        distinct.len() > 6,
        "focus barely varied across peers ({} distinct sets of 12): selection is \
         not effectively keyed on the per-peer salt",
        distinct.len()
    );
}

#[test]
fn focus_handles_having_fewer_contracts_than_slots() {
    let selector = FocusSelector::new([1; 32], 4);
    assert!(selector.select(&[]).is_empty());
    assert_eq!(selector.select(&[instance(1)]).len(), 1);
}

#[test]
fn duplicate_candidates_do_not_waste_focus_slots() {
    let selector = FocusSelector::new([1; 32], 2);
    let selected = selector.select(&[instance(1), instance(1), instance(2)]);
    assert_eq!(selected.len(), 2);
    assert_ne!(selected[0], selected[1]);
}

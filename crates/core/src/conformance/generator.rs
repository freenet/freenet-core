//! Turning a corpus of observed states into cases worth checking.
//!
//! Passing a finite corpus cannot prove a contract correct, but one reproducible
//! counterexample proves a defect. So the generator's job is coverage of *kinds* of
//! failure, not exhaustiveness.

use std::sync::Arc;

use freenet_stdlib::prelude::RelatedContracts;

use super::property::ConformanceProperty;
use super::verifier::{Bytes, ConformanceCase};

/// The material a generator works from: states and deltas actually observed, plus
/// whatever related-contract state is needed to make them validate.
///
/// # The two provenance fields
///
/// `delta_bases` and `transitions` are the ONLY fields here that carry provenance —
/// facts about how the bytes were observed rather than the bytes themselves. Evidence
/// files carry bare `states`, `deltas` and `summaries` and nothing else, so provenance
/// is precisely what a recipient re-executing a case cannot reconstruct. Hence the
/// structural rule, stated in full on
/// [`ConformanceProperty::premise_source`](super::property::ConformanceProperty::premise_source):
///
/// > **A property whose generator branch reads `delta_bases` or `transitions` must be
/// > [`PremiseSource::LocalProvenance`](super::property::PremiseSource::LocalProvenance).**
///
/// Adding a third provenance field means extending that rule, not just this struct.
#[derive(Debug, Default, Clone)]
pub struct Corpus {
    pub states: Vec<Bytes>,
    pub deltas: Vec<Bytes>,
    /// The state each delta was observed being applied to, where that is known.
    ///
    /// Parallel to `deltas`; shorter (or empty) means the tail has no known base.
    /// Provenance is what makes a permutation check meaningful: a delta is computed
    /// as `get_state_delta(sender_state, recipient_summary)`, so two deltas observed
    /// against DIFFERENT bases may be causally sequenced — the second computed from a
    /// state that already contains the first. Permuting those two is a situation the
    /// protocol never produces, and a "violation" from it would be an artifact of how
    /// the case was built rather than a defect in the contract.
    ///
    /// Two deltas observed against the SAME base were both legitimately applicable to
    /// that state, which is precisely the concurrent-independent-updates scenario the
    /// law is about.
    pub delta_bases: Vec<Option<Bytes>>,
    pub summaries: Vec<Bytes>,
    /// Observed `(base, result)` steps: a peer held `base`, applied an update, and
    /// ended up at `result`.
    ///
    /// This is PROVENANCE, and it is the whole reason
    /// [`ConformanceProperty::TransitionPathAgreement`] is a law rather than an
    /// accusation of last-write-wins. For an arbitrary pair of states, "merging B
    /// into A yields B" is false for every conforming contract; it is only required
    /// when the corpus witnesses that B was reached FROM A.
    ///
    /// So this must never be filled from states that merely appeared together. A
    /// bundle's `transitions` and the sampler's own transition records are the only
    /// sources; loose `--state` files carry no provenance and contribute none.
    pub transitions: Vec<(Bytes, Bytes)>,
    pub related: RelatedContracts<'static>,
}

impl Corpus {
    pub fn from_states(states: Vec<Vec<u8>>) -> Self {
        Self {
            states: states
                .into_iter()
                .map(|s| Arc::from(s.as_slice()))
                .collect(),
            ..Default::default()
        }
    }

    /// Drop duplicate states, keeping first-seen order.
    ///
    /// A contract oscillating between two states will offer the same handful of
    /// byte strings over and over; without this, a corpus of a thousand
    /// observations is a corpus of two.
    pub fn deduplicated(mut self) -> Self {
        self.states = dedup(self.states);
        // Deltas dedup together with their bases, or the two lists drift out of step
        // and a delta inherits some other delta's provenance — which would put the
        // confound back while looking like it had been fixed.
        let (deltas, bases) = dedup_with_bases(self.deltas, self.delta_bases);
        self.deltas = deltas;
        self.delta_bases = bases;
        self.summaries = dedup(self.summaries);
        // A contract oscillating between two states offers the same step over and
        // over; without this a thousand observations are one case repeated.
        let mut seen = std::collections::HashSet::new();
        self.transitions.retain(|(base, result)| {
            seen.insert((
                *blake3::hash(base).as_bytes(),
                *blake3::hash(result).as_bytes(),
            ))
        });
        self
    }

    /// The base `deltas[i]` was observed against, if known.
    pub fn delta_base(&self, i: usize) -> Option<&Bytes> {
        self.delta_bases.get(i).and_then(Option::as_ref)
    }

    /// Nothing here can produce a case.
    ///
    /// Transitions are counted as well as states. A corpus can legitimately carry
    /// steps and no loose states (`fdev --transition` pushes both endpoints into
    /// `states` today, but the sampler's own records and any future caller need
    /// not), and a states-only test would short-circuit `generate_cases` before the
    /// transition queue was ever built, so the one property that depends on
    /// provenance would silently check nothing.
    pub fn is_empty(&self) -> bool {
        self.states.is_empty() && self.transitions.is_empty()
    }
}

/// Deduplicate deltas while keeping each one's base alongside it.
fn dedup_with_bases(
    deltas: Vec<Bytes>,
    bases: Vec<Option<Bytes>>,
) -> (Vec<Bytes>, Vec<Option<Bytes>>) {
    let mut seen: std::collections::HashMap<[u8; 32], usize> = std::collections::HashMap::new();
    let mut kept_deltas: Vec<Bytes> = Vec::with_capacity(deltas.len());
    let mut kept_bases: Vec<Option<Bytes>> = Vec::with_capacity(deltas.len());
    for (i, delta) in deltas.into_iter().enumerate() {
        let base = bases.get(i).cloned().flatten();
        match seen.entry(*blake3::hash(&delta).as_bytes()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(kept_deltas.len());
                kept_bases.push(base);
                kept_deltas.push(delta);
            }
            // First-seen order still decides which copy is KEPT, but a later copy
            // that carries provenance upgrades the one already kept.
            //
            // Dropping to the first copy unconditionally is how a delta that appears
            // both loose and on a transition — which is exactly what a bundle
            // carrying both looks like — ends up unprovenanced, and an unprovenanced
            // delta is never paired at all, so the permutation law silently checks
            // nothing. Two identical delta byte strings observed against different
            // bases already had an arbitrary one chosen; None to Some is strictly
            // more information than that.
            std::collections::hash_map::Entry::Occupied(slot) => {
                let at = *slot.get();
                if kept_bases[at].is_none() {
                    kept_bases[at] = base;
                }
            }
        }
    }
    (kept_deltas, kept_bases)
}

fn dedup(items: Vec<Bytes>) -> Vec<Bytes> {
    let mut seen = std::collections::HashSet::new();
    items
        .into_iter()
        .filter(|item| seen.insert(*blake3::hash(item).as_bytes()))
        .collect()
}

#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Upper bound on cases produced. Cases are interleaved across properties, so
    /// truncating here narrows depth rather than dropping whole laws.
    pub max_cases: usize,
    /// Which laws to check. Defaults to all of them.
    pub properties: Vec<ConformanceProperty>,
    /// Skip any case whose inputs exceed this. Keeps a handful of very large states
    /// from dominating the run.
    pub max_case_bytes: usize,
    /// Cap on states considered for pairing, since pairs grow quadratically.
    pub max_states_paired: usize,
    /// Cap on recorded steps considered for `TransitionPathAgreement`.
    ///
    /// The transition branch does not pair anything, so it does not grow
    /// quadratically - but it is linear in a corpus a busy contract fills without
    /// limit, and every other arity branch is bounded. An unbounded queue here would
    /// let one contract's step history crowd the interleave and decide which laws
    /// the case budget reaches, which is exactly what the interleave exists to
    /// prevent.
    pub max_transitions: usize,
    pub seed: u64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            max_cases: 512,
            properties: ConformanceProperty::ALL.to_vec(),
            max_case_bytes: 4 * 1024 * 1024,
            max_states_paired: 24,
            max_transitions: 24,
            seed: 0,
        }
    }
}

/// Build cases from a corpus.
///
/// Deterministic given `(corpus, config)`: the same inputs produce the same cases in
/// the same order, so a run that finds something can be re-run and a run that finds
/// nothing can be compared against a later one.
pub fn generate_cases(corpus: &Corpus, config: &GeneratorConfig) -> Vec<ConformanceCase> {
    if corpus.is_empty() {
        return Vec::new();
    }

    // Per-property queues, filled independently and then interleaved. The
    // interleave is the point: a corpus big enough to blow the case budget on
    // commutativity alone would otherwise never reach associativity, and the
    // budget would silently decide which laws get checked.
    let mut queues: Vec<Vec<ConformanceCase>> = Vec::new();
    for property in &config.properties {
        queues.push(cases_for(*property, corpus, config));
    }

    let mut out = Vec::new();
    let mut round = 0usize;
    loop {
        let mut produced = false;
        for queue in &mut queues {
            if round >= queue.len() {
                continue;
            }
            produced = true;
            out.push(queue[round].clone());
            if out.len() >= config.max_cases {
                return out;
            }
        }
        if !produced {
            return out;
        }
        round += 1;
    }
}

fn cases_for(
    property: ConformanceProperty,
    corpus: &Corpus,
    config: &GeneratorConfig,
) -> Vec<ConformanceCase> {
    let states = paired_states(corpus, config);
    let mut cases = Vec::new();
    let mut push = |case: ConformanceCase| {
        if case.input_bytes() <= config.max_case_bytes {
            cases.push(case);
        }
    };

    let build = |states: Vec<Bytes>| {
        ConformanceCase::new(property, states).with_related(corpus.related.clone())
    };

    // Delta generation against an OBSERVED summary, not only a self-summary.
    //
    // Handled before the arity match because it is the one property whose useful
    // cases depend on captured summaries. Without it those summaries are dead weight
    // in every corpus: the verifier falls back to `summarize(state)`, which only ever
    // exercises the "peer is exactly up to date" case. The defects that matter live
    // in `get_state_delta(state, a summary from a peer at a different point)`, which
    // is the call the network actually makes.
    if property == ConformanceProperty::DeltaDeterminism {
        for state in &states {
            push(build(vec![state.clone()]));
            for summary in &corpus.summaries {
                push(build(vec![state.clone()]).with_summary(summary.clone()));
            }
        }
        return cases;
    }

    // Transition cases come from recorded provenance, never from pairing states.
    //
    // Handled before the arity match for the same reason `DeltaDeterminism` is: the
    // generic arity-2 branch pairs every state with every other, and this property
    // is only a law for the ORDERED pair a transition witnesses. Falling through to
    // that branch would emit `(A, B)` for every pair in the corpus and accuse every
    // conforming contract of last-write-wins.
    if property == ConformanceProperty::TransitionPathAgreement {
        for (base, result) in sampled_transitions(corpus, config) {
            push(build(vec![base, result]));
        }
        return cases;
    }

    match property.state_arity() {
        1 if property.delta_arity() == 0 => {
            for state in &states {
                push(build(vec![state.clone()]));
            }
        }
        1 => {
            // Delta properties: cross every state with the observed deltas. A delta
            // that does not apply to a given state comes back inconclusive, which
            // costs one call and is the honest answer.
            for state in &states {
                for pair in delta_pairs(corpus, property.delta_arity()) {
                    push(build(vec![state.clone()]).with_deltas(pair).clone());
                }
            }
        }
        2 => {
            for (i, a) in states.iter().enumerate() {
                for b in states.iter().skip(i + 1) {
                    push(build(vec![a.clone(), b.clone()]));
                }
            }
        }
        3 => {
            // Triples grow cubically, so walk them on a stride rather than
            // exhaustively: an associativity defect that appears for exactly one
            // triple in thousands was never going to be caught by a bounded run,
            // and pretending otherwise just crowds out the other laws.
            //
            // The third element is picked by arithmetic rather than by an RNG. Not
            // squeamishness about `rand`: a corpus replayed on another peer must
            // produce the identical case list, or evidence found here is not
            // reproducible there, and that is the whole contract of this module.
            let n = states.len();
            if n >= 3 {
                let target = config.max_cases.min(n.saturating_mul(2));
                let mut emitted = 0;
                'outer: for i in 0..n {
                    for j in 0..n {
                        if emitted >= target {
                            break 'outer;
                        }
                        if i == j {
                            continue;
                        }
                        let k = (i + j + 1 + config.seed as usize) % n;
                        if k == i || k == j {
                            continue;
                        }
                        push(build(vec![
                            states[i].clone(),
                            states[j].clone(),
                            states[k].clone(),
                        ]));
                        emitted += 1;
                    }
                }
            }
        }
        _ => {}
    }

    cases
}

/// Deterministically choose which states to pair up.
///
/// When the corpus is larger than the pairing cap, take a strided sample rather than
/// the first N: observations arrive in time order, so the first N are all from the
/// same few minutes and would test a contract only against its own recent past.
fn paired_states(corpus: &Corpus, config: &GeneratorConfig) -> Vec<Bytes> {
    let n = corpus.states.len();
    if n <= config.max_states_paired {
        return corpus.states.clone();
    }
    let stride = n as f64 / config.max_states_paired as f64;
    (0..config.max_states_paired)
        .map(|i| corpus.states[((i as f64) * stride) as usize % n].clone())
        .collect()
}

/// Deterministically choose which recorded steps to check.
///
/// Strided rather than truncated, for the same reason [`paired_states`] strides:
/// observations arrive in time order, so the first N steps are all from the same few
/// minutes and would test a contract only against its own recent past.
fn sampled_transitions(corpus: &Corpus, config: &GeneratorConfig) -> Vec<(Bytes, Bytes)> {
    let n = corpus.transitions.len();
    if n <= config.max_transitions {
        return corpus.transitions.clone();
    }
    let stride = n as f64 / config.max_transitions as f64;
    (0..config.max_transitions)
        .map(|i| corpus.transitions[((i as f64) * stride) as usize % n].clone())
        .collect()
}

fn delta_pairs(corpus: &Corpus, arity: usize) -> Vec<Vec<Bytes>> {
    if corpus.deltas.len() < arity {
        return Vec::new();
    }
    match arity {
        1 => corpus.deltas.iter().map(|d| vec![d.clone()]).collect(),
        2 => {
            // Only pair deltas observed against the SAME base state.
            //
            // Crossing every pair meant pairing deltas that may be causally
            // sequenced: a delta is computed from its sender's state, so one observed
            // against a later base can already contain the other's effect, and
            // applying them in the reverse order is a situation the protocol never
            // produces. A finding from such a pair says more about how the case was
            // built than about the contract.
            //
            // Deltas with no recorded base are not paired at all. Reporting nothing
            // is the right answer when the inputs cannot support the question; the
            // alternative is a finding nobody can act on.
            let mut pairs = Vec::new();
            for (i, a) in corpus.deltas.iter().enumerate() {
                let Some(base_a) = corpus.delta_base(i) else {
                    continue;
                };
                for (j, b) in corpus.deltas.iter().enumerate().skip(i + 1) {
                    let Some(base_b) = corpus.delta_base(j) else {
                        continue;
                    };
                    if base_a == base_b {
                        pairs.push(vec![a.clone(), b.clone()]);
                    }
                }
            }
            pairs
        }
        _ => Vec::new(),
    }
}

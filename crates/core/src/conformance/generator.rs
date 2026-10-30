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
    ///
    /// A skipped case never runs, so a property can read as held while the cases
    /// that would break it were the ones skipped. A contract that is wrong only near
    /// its size cap is exactly that shape (#5705). [`generate_cases_counted`]
    /// counts the cases skipped for size and for the case budget per property, and
    /// `fdev verify-merge` reports them. Cases left out by state sampling are not
    /// counted; only the number of states sampled is. The shadow prober still uses the uncounted [`generate_cases`]; see #6044.
    pub max_case_bytes: usize,
    /// Cap on states the state-based laws use, since pairs grow quadratically.
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

/// What [`generate_cases_counted`] built, and what it refused to build.
#[derive(Debug, Default, Clone)]
pub struct GeneratedCases {
    pub cases: Vec<ConformanceCase>,
    /// One entry per entry of [`GeneratorConfig::properties`], in that order: how
    /// many cases were built for the property and then dropped because their inputs
    /// exceeded [`GeneratorConfig::max_case_bytes`].
    ///
    /// Zero entries are kept, so a consumer can tell "nothing was skipped" from
    /// "this property was not counted".
    pub oversized: Vec<(ConformanceProperty, usize)>,
    /// Parallel to `oversized`: cases that passed the size limit and were then left
    /// out because the [`GeneratorConfig::max_cases`] budget ran out first.
    ///
    /// Disjoint from `oversized`, so the two add up to every built case that did not
    /// run. Raising the size limit can move cases from `oversized` to here, which is
    /// why the budget needs its own count.
    pub budget_cut: Vec<(ConformanceProperty, usize)>,
    /// How many states the state-based laws used, out of `corpus.states.len()`.
    /// Below the total when the corpus exceeds [`GeneratorConfig::max_states_paired`].
    pub states_paired: usize,
    /// How many recorded transitions were used, out of `corpus.transitions.len()`.
    /// Below the total when the corpus exceeds [`GeneratorConfig::max_transitions`].
    pub transitions_sampled: usize,
}

impl GeneratedCases {
    /// Over-size cases across every property.
    pub fn oversized_total(&self) -> usize {
        self.oversized.iter().map(|(_, n)| n).sum()
    }

    /// Budget-cut cases across every property.
    pub fn budget_cut_total(&self) -> usize {
        self.budget_cut.iter().map(|(_, n)| n).sum()
    }
}

/// Build cases from a corpus, discarding the count of over-size cases skipped.
///
/// A thin wrapper over [`generate_cases_counted`] for callers that only need the
/// cases. A caller that reports a verdict to anyone must use the counted form:
/// without the count, "every case held" cannot be told apart from "the cases that
/// would have failed were too large to run" (#5705).
pub fn generate_cases(corpus: &Corpus, config: &GeneratorConfig) -> Vec<ConformanceCase> {
    generate_cases_counted(corpus, config).cases
}

/// Build cases from a corpus, and count the cases skipped for size and for budget.
///
/// Deterministic given `(corpus, config)`: the same inputs produce the same cases in
/// the same order, so a run that finds something can be re-run and a run that finds
/// nothing can be compared against a later one.
pub fn generate_cases_counted(corpus: &Corpus, config: &GeneratorConfig) -> GeneratedCases {
    let zeros = || config.properties.iter().map(|p| (*p, 0)).collect();
    if corpus.is_empty() {
        return GeneratedCases {
            cases: Vec::new(),
            oversized: zeros(),
            budget_cut: zeros(),
            states_paired: 0,
            transitions_sampled: 0,
        };
    }

    // Per-property queues, filled independently and then interleaved. The
    // interleave is the point: a corpus big enough to blow the case budget on
    // commutativity alone would otherwise never reach associativity, and the
    // budget would silently decide which laws get checked.
    let mut queues: Vec<Vec<ConformanceCase>> = Vec::new();
    let mut oversized = Vec::with_capacity(config.properties.len());
    for property in &config.properties {
        let (queue, skipped) = cases_for(*property, corpus, config);
        queues.push(queue);
        oversized.push((*property, skipped));
    }

    let mut cases = Vec::new();
    let mut taken = vec![0usize; queues.len()];
    let mut round = 0usize;
    'interleave: loop {
        let mut produced = false;
        for (queue, taken) in queues.iter().zip(&mut taken) {
            if round >= queue.len() {
                continue;
            }
            produced = true;
            cases.push(queue[round].clone());
            *taken += 1;
            if cases.len() >= config.max_cases {
                break 'interleave;
            }
        }
        if !produced {
            break;
        }
        round += 1;
    }

    let budget_cut = config
        .properties
        .iter()
        .zip(queues.iter().zip(&taken))
        .map(|(p, (queue, taken))| (*p, queue.len() - taken))
        .collect();
    GeneratedCases {
        cases,
        oversized,
        budget_cut,
        states_paired: paired_states(corpus, config).len(),
        transitions_sampled: sampled_transitions(corpus, config).len(),
    }
}

/// The cases for one property, and how many were dropped for exceeding
/// [`GeneratorConfig::max_case_bytes`].
fn cases_for(
    property: ConformanceProperty,
    corpus: &Corpus,
    config: &GeneratorConfig,
) -> (Vec<ConformanceCase>, usize) {
    let states = paired_states(corpus, config);
    let mut cases = Vec::new();
    // Counted here, at the only place a case is dropped for size. A drop that is not
    // counted renders as a clean zero: the report would show only the cases that ran,
    // and a property would read as held when its breaking cases never ran (#5705).
    let mut oversized = 0usize;
    let mut push = |case: ConformanceCase| {
        if case.input_bytes() <= config.max_case_bytes {
            cases.push(case);
        } else {
            oversized += 1;
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
        return (cases, oversized);
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
        return (cases, oversized);
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

    (cases, oversized)
}

/// Deterministically choose which states the state-based laws use, in time order.
///
/// When the corpus is larger than the cap, take a strided sample rather than the
/// first N: observations arrive in time order, so the first N are all from the same
/// few minutes and would test a contract only against its own recent past.
///
/// The last-supplied state and the largest state are always in the sample. For a
/// growing contract those are the states nearest its size cap, which a plain stride
/// never reached (#5705).
///
/// Time order is load-bearing: pairs are built `(earlier, later)`, so a later state
/// is the incoming side of the merge, which is the direction the network uses and
/// the only one the one-directional laws check. Queue order is #6053.
fn paired_states(corpus: &Corpus, config: &GeneratorConfig) -> Vec<Bytes> {
    let states = &corpus.states;
    let Some(last) = states.len().checked_sub(1) else {
        return Vec::new();
    };
    // Ties go to the later state.
    let largest = (0..states.len())
        .max_by_key(|&i| (states[i].len(), i))
        .unwrap_or(last);
    sample_indices(states.len(), config.max_states_paired, &[last, largest])
        .into_iter()
        .map(|i| states[i].clone())
        .collect()
}

/// Indices of a strided sample of at most `cap` of `n` items, ascending, that
/// includes every index in `keep` that there is room for.
///
/// The plain stride `i * n / cap` never reaches `n - 1` once `n > cap`. Each kept
/// index replaces the nearest stride index that is not itself kept, so the sample
/// stays spread and stays distinct. With `cap` smaller than `keep`, not every kept
/// index fits: one that is already on the stride stays, and the remaining slots go
/// to the others in `keep` order (so `sample_indices(10, 1, &[9, 0])` is `[0]`).
pub(super) fn sample_indices(n: usize, cap: usize, keep: &[usize]) -> Vec<usize> {
    if n <= cap {
        return (0..n).collect();
    }
    // Strictly increasing, since `n > cap`.
    let mut picked: Vec<usize> = (0..cap).map(|i| i * n / cap).collect();
    for &k in keep {
        if picked.contains(&k) {
            continue;
        }
        let slot = (0..picked.len())
            .filter(|&p| !keep.contains(&picked[p]))
            .min_by_key(|&p| picked[p].abs_diff(k));
        if let Some(slot) = slot {
            picked[slot] = k;
        }
    }
    picked.sort_unstable();
    picked
}

/// Deterministically choose which recorded steps to check.
///
/// Strided rather than truncated, for the same reason [`paired_states`] strides:
/// observations arrive in time order, so the first N steps are all from the same few
/// minutes and would test a contract only against its own recent past. The last
/// supplied step is always included.
fn sampled_transitions(corpus: &Corpus, config: &GeneratorConfig) -> Vec<(Bytes, Bytes)> {
    let n = corpus.transitions.len();
    let keep = n.checked_sub(1).map(|last| vec![last]).unwrap_or_default();
    sample_indices(n, config.max_transitions, &keep)
        .into_iter()
        .map(|i| corpus.transitions[i].clone())
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

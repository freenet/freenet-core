//! Turning a corpus of observed states into cases worth checking.
//!
//! Passing a finite corpus cannot prove a contract correct, but one reproducible
//! counterexample proves a defect. So the generator's job is coverage of *kinds* of
//! failure, not exhaustiveness.

use std::rc::Rc;
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
    /// `fdev verify-merge` reports them, along with the cases sampling left out
    /// ([`GeneratedCases::not_sampled`]), as does the shadow prober.
    ///
    /// Raising this is not purely additive for associativity: its cap then fills
    /// with triples containing the big states, which are queued first, so triples of
    /// small states that ran before may no longer run.
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

#[cfg(test)]
thread_local! {
    /// Cases the generator has constructed on this thread, so a test can check that
    /// the work done tracks the case budget rather than the size of the queue.
    pub(super) static CASES_BUILT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What [`generate_cases_counted`] built, and what it refused to build.
#[derive(Debug, Default, Clone)]
pub struct GeneratedCases {
    pub cases: Vec<ConformanceCase>,
    /// One entry per entry of [`GeneratorConfig::properties`], in that order: how
    /// many of the cases offered to the property were dropped because their inputs
    /// exceeded [`GeneratorConfig::max_case_bytes`].
    ///
    /// For associativity this counts the triples over the limit among those walked.
    /// The walk goes on past them until its cap of `2n` can run, so at most `2n` of
    /// them could run with a higher limit.
    ///
    /// Zero entries are kept, so a consumer can tell "nothing was skipped" from
    /// "this property was not counted".
    pub oversized: Vec<(ConformanceProperty, usize)>,
    /// Parallel to `oversized`: cases that passed the size limit and were then left
    /// out because the [`GeneratorConfig::max_cases`] budget ran out first.
    ///
    /// Disjoint from `oversized`, so the two add up to every offered case that did
    /// not run. Raising the size limit can move cases from `oversized` to here, which is
    /// why the budget needs its own count.
    pub budget_cut: Vec<(ConformanceProperty, usize)>,
    /// Parallel to `oversized`: cases never offered at all, because the state or
    /// transition sample left their inputs out, or because the associativity walk
    /// stopped at its cap. What the property would have had with every state and
    /// transition in the corpus, minus what it was offered. Disjoint from the size
    /// and budget counts, which are taken over the cases that were offered.
    pub not_sampled: Vec<(ConformanceProperty, usize)>,
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

    /// Unsampled cases across every property.
    pub fn not_sampled_total(&self) -> usize {
        self.not_sampled
            .iter()
            .fold(0usize, |total, (_, n)| total.saturating_add(*n))
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
///
/// Each property's cases are described as [`Spec`]s first and built only when the
/// budget takes them, so the work done tracks [`GeneratorConfig::max_cases`] rather
/// than the size of the queue. The specs past the budget are still walked, cheaply,
/// so the size and budget counts stay exact.
pub fn generate_cases_counted(corpus: &Corpus, config: &GeneratorConfig) -> GeneratedCases {
    let zeros = || config.properties.iter().map(|p| (*p, 0)).collect();
    if corpus.is_empty() {
        return GeneratedCases {
            cases: Vec::new(),
            oversized: zeros(),
            budget_cut: zeros(),
            not_sampled: zeros(),
            states_paired: 0,
            transitions_sampled: 0,
        };
    }

    let inputs = Inputs::new(corpus, config);
    // Per-property queues, drawn from in turn. The interleave is the point: a
    // corpus big enough to blow the case budget on commutativity alone would
    // otherwise never reach associativity, and the budget would silently decide
    // which laws get checked.
    let mut queues: Vec<_> = config
        .properties
        .iter()
        .map(|p| specs(*p, &inputs, config))
        .collect();
    let mut oversized = vec![0usize; queues.len()];
    // Counted here, at the only place a case is dropped for size. A drop that is not
    // counted renders as a clean zero: the report would show only the cases that ran,
    // and a property would read as held when its breaking cases never ran (#5705).
    let mut next_within_size = |q: usize, queues: &mut Vec<Box<dyn Iterator<Item = Spec> + '_>>| {
        for spec in queues[q].by_ref() {
            if inputs.size(&spec) <= config.max_case_bytes {
                return Some(spec);
            }
            oversized[q] += 1;
        }
        None
    };

    let mut cases = Vec::new();
    let mut taken = vec![0usize; queues.len()];
    let mut live = vec![true; queues.len()];
    'interleave: while live.iter().any(|l| *l) {
        for (q, property) in config.properties.iter().enumerate() {
            if !live[q] {
                continue;
            }
            match next_within_size(q, &mut queues) {
                Some(spec) => {
                    cases.push(inputs.build(*property, &spec));
                    taken[q] += 1;
                }
                None => live[q] = false,
            }
            if cases.len() >= config.max_cases {
                break 'interleave;
            }
        }
    }

    // The budget is spent: walk what is left without building it.
    let mut budget_cut = vec![0usize; queues.len()];
    for (q, cut) in budget_cut.iter_mut().enumerate() {
        while next_within_size(q, &mut queues).is_some() {
            *cut += 1;
        }
    }

    let not_sampled = config
        .properties
        .iter()
        .enumerate()
        .map(|(q, p)| {
            let offered = taken[q]
                .saturating_add(oversized[q])
                .saturating_add(budget_cut[q]);
            unsampled_total(*p, corpus).saturating_sub(offered)
        })
        .collect();
    let pair = |counts: Vec<usize>| config.properties.iter().copied().zip(counts).collect();
    GeneratedCases {
        cases,
        oversized: pair(oversized),
        budget_cut: pair(budget_cut),
        not_sampled: pair(not_sampled),
        states_paired: inputs.states.len(),
        transitions_sampled: inputs.transitions.len(),
    }
}

/// How many cases `property` would have over the whole corpus, with no state or
/// transition sampling and no cap on the associativity walk.
fn unsampled_total(property: ConformanceProperty, corpus: &Corpus) -> usize {
    let n = corpus.states.len();
    if property == ConformanceProperty::DeltaDeterminism {
        return n.saturating_mul(1 + corpus.summaries.len());
    }
    if property == ConformanceProperty::TransitionPathAgreement {
        return corpus.transitions.len();
    }
    match property.state_arity() {
        1 if property.delta_arity() == 0 => n,
        1 => n.saturating_mul(delta_pairs(corpus, property.delta_arity()).len()),
        2 => n.saturating_mul(n.saturating_sub(1)) / 2,
        // The triples the `(i, j, (i + j + 1 + seed) % n)` rule can yield at all,
        // for any seed: of the `n(n - 1)` ordered `(i, j)`, exactly `n - 1` give
        // `k == i` and another `n - 1` give `k == j`, and no pair gives both.
        3 if n >= 3 => (n - 1).saturating_mul(n - 2),
        _ => 0,
    }
}

/// One case as indices into the inputs it is built from, so its size is known and
/// a case the budget cuts can be counted without being built.
enum Spec {
    /// Positions in [`Inputs::states`], in the case's own element order.
    States(Vec<usize>),
    /// A position in [`Inputs::states`] and indices into `corpus.deltas`.
    StateDeltas(usize, Rc<[usize]>),
    /// A position in [`Inputs::states`] and an optional index into
    /// `corpus.summaries`.
    StateSummary(usize, Option<usize>),
    /// A position in [`Inputs::transitions`].
    Transition(usize),
}

/// The sampled inputs every property's cases are drawn from.
struct Inputs<'a> {
    corpus: &'a Corpus,
    /// The state sample, in corpus order. Pairs are built `(earlier, later)` from
    /// it, so a later state is the incoming side of the merge, which is the
    /// direction the network uses and the only one the one-directional laws check.
    states: Vec<Bytes>,
    transitions: Vec<(Bytes, Bytes)>,
    /// Positions in `states` in the order their cases are queued: the last-supplied
    /// state, then the largest, then the rest newest first. The budget cuts from
    /// the end of each queue, so it cuts the cases of the states earliest in the
    /// corpus (#6053). Corpus order is usually time order, but not always: a shadow
    /// corpus interleaves sampler strata, and a bundle lists its transition states
    /// after its loose ones.
    /// Queue order only; it never changes a case's element order.
    priority: Vec<usize>,
}

impl<'a> Inputs<'a> {
    fn new(corpus: &'a Corpus, config: &GeneratorConfig) -> Self {
        let all = &corpus.states;
        let (indices, largest) = match all.len().checked_sub(1) {
            None => (Vec::new(), None),
            Some(last) => {
                // Ties go to the later state.
                let largest = (0..all.len())
                    .max_by_key(|&i| (all[i].len(), i))
                    .unwrap_or(last);
                (
                    sample_indices(all.len(), config.max_states_paired, &[last, largest]),
                    Some(largest),
                )
            }
        };
        let n = indices.len();
        let mut priority: Vec<usize> = Vec::with_capacity(n);
        if n > 0 {
            priority.push(n - 1);
        }
        if let Some(at) = largest.and_then(|l| indices.iter().position(|&i| i == l)) {
            if !priority.contains(&at) {
                priority.push(at);
            }
        }
        let rest: Vec<usize> = (0..n).rev().filter(|p| !priority.contains(p)).collect();
        priority.extend(rest);
        Self {
            corpus,
            states: indices.into_iter().map(|i| all[i].clone()).collect(),
            transitions: sampled_transitions(corpus, config),
            priority,
        }
    }

    /// The total size of the sampled states at these positions.
    fn states_size(&self, at: &[usize]) -> usize {
        at.iter().map(|&a| self.states[a].len()).sum()
    }

    /// What [`ConformanceCase::input_bytes`] would return for the built case.
    fn size(&self, spec: &Spec) -> usize {
        match spec {
            Spec::States(at) => self.states_size(at),
            Spec::StateDeltas(at, deltas) => {
                self.states[*at].len()
                    + deltas
                        .iter()
                        .map(|&d| self.corpus.deltas[d].len())
                        .sum::<usize>()
            }
            Spec::StateSummary(at, summary) => {
                self.states[*at].len() + summary.map_or(0, |m| self.corpus.summaries[m].len())
            }
            Spec::Transition(t) => self.transitions[*t].0.len() + self.transitions[*t].1.len(),
        }
    }

    fn build(&self, property: ConformanceProperty, spec: &Spec) -> ConformanceCase {
        #[cfg(test)]
        CASES_BUILT.with(|n| n.set(n.get() + 1));
        let case = |states: Vec<Bytes>| {
            ConformanceCase::new(property, states).with_related(self.corpus.related.clone())
        };
        match spec {
            Spec::States(at) => case(at.iter().map(|&a| self.states[a].clone()).collect()),
            Spec::StateDeltas(at, deltas) => case(vec![self.states[*at].clone()]).with_deltas(
                deltas
                    .iter()
                    .map(|&d| self.corpus.deltas[d].clone())
                    .collect(),
            ),
            Spec::StateSummary(at, summary) => {
                let built = case(vec![self.states[*at].clone()]);
                match summary {
                    Some(m) => built.with_summary(self.corpus.summaries[*m].clone()),
                    None => built,
                }
            }
            Spec::Transition(t) => {
                let (base, result) = self.transitions[*t].clone();
                case(vec![base, result])
            }
        }
    }
}

/// One property's cases, in queue order, as a lazy stream of [`Spec`]s.
fn specs<'a>(
    property: ConformanceProperty,
    inputs: &'a Inputs<'a>,
    config: &GeneratorConfig,
) -> Box<dyn Iterator<Item = Spec> + 'a> {
    let priority = &inputs.priority;
    let n = priority.len();

    // Delta generation against an OBSERVED summary, not only a self-summary.
    //
    // Handled before the arity match because it is the one property whose useful
    // cases depend on captured summaries. Without it those summaries are dead weight
    // in every corpus: the verifier falls back to `summarize(state)`, which only ever
    // exercises the "peer is exactly up to date" case. The defects that matter live
    // in `get_state_delta(state, a summary from a peer at a different point)`, which
    // is the call the network actually makes. Newest summary first.
    if property == ConformanceProperty::DeltaDeterminism {
        let summaries = inputs.corpus.summaries.len();
        return Box::new(priority.iter().flat_map(move |&at| {
            std::iter::once(Spec::StateSummary(at, None)).chain(
                (0..summaries)
                    .rev()
                    .map(move |m| Spec::StateSummary(at, Some(m))),
            )
        }));
    }

    // Transition cases come from recorded provenance, never from pairing states.
    //
    // Handled before the arity match for the same reason `DeltaDeterminism` is: the
    // generic arity-2 branch pairs every state with every other, and this property
    // is only a law for the ORDERED pair a transition witnesses. Falling through to
    // that branch would emit `(A, B)` for every pair in the corpus and accuse every
    // conforming contract of last-write-wins. Last-supplied transition first.
    if property == ConformanceProperty::TransitionPathAgreement {
        return Box::new((0..inputs.transitions.len()).rev().map(Spec::Transition));
    }

    match property.state_arity() {
        1 if property.delta_arity() == 0 => {
            Box::new(priority.iter().map(|&at| Spec::States(vec![at])))
        }
        1 => {
            // Delta properties: cross every state with the observed deltas. A delta
            // that does not apply to a given state comes back inconclusive, which
            // costs one call and is the honest answer.
            let pairs: Rc<[Rc<[usize]>]> = delta_pairs(inputs.corpus, property.delta_arity())
                .into_iter()
                .map(Rc::from)
                .collect();
            Box::new(priority.iter().flat_map(move |&at| {
                let pairs = pairs.clone();
                (0..pairs.len()).map(move |d| Spec::StateDeltas(at, pairs[d].clone()))
            }))
        }
        2 => {
            // Every pair, queued by the higher-priority state it contains, and always
            // built `(earlier, later)`.
            Box::new((0..n).flat_map(move |a| {
                (a + 1..n).map(move |b| {
                    let (x, y) = (priority[a], priority[b]);
                    Spec::States(vec![x.min(y), x.max(y)])
                })
            }))
        }
        3 => {
            let max_case_bytes = config.max_case_bytes;
            let fits = move |triple: &[usize; 3]| inputs.states_size(triple) <= max_case_bytes;
            let target = config.max_cases.min(n.saturating_mul(2));
            Box::new(
                triples(priority, config.seed, target, fits)
                    .map(|triple| Spec::States(triple.to_vec())),
            )
        }
        _ => Box::new(std::iter::empty()),
    }
}

/// [`triples`] as plain index triples, so a test can compare the walk with an
/// independent enumeration of the rule.
#[cfg(test)]
pub(super) fn triple_walk(priority: &[usize], seed: u64, target: usize) -> Vec<[usize; 3]> {
    triples(priority, seed, target, |_| true).collect()
}

/// The associativity triples, until `target` of them (`min(max_cases, 2n)`) can run.
///
/// Triples grow cubically, so walk them on a stride rather than exhaustively: an
/// associativity defect that appears for exactly one triple in thousands was never
/// going to be caught by a bounded run, and pretending otherwise just crowds out the
/// other laws.
///
/// The third element is picked by arithmetic rather than by an RNG. Not squeamishness
/// about `rand`: a corpus replayed on another peer must produce the identical case
/// list, or evidence found here is not reproducible there, and that is the whole
/// contract of this module. Every triple is `(i, j, (i + j + 1 + seed) % n)` in
/// positions in the sample, which is in corpus order.
///
/// Queued by the highest-priority state a triple contains: for each state in
/// [`Inputs::priority`] order, the triples in which it is the highest-priority
/// member, in every slot it can hold under the rule above.
fn triples<'a>(
    priority: &'a [usize],
    seed: u64,
    target: usize,
    fits: impl Fn(&[usize; 3]) -> bool + 'a,
) -> Box<dyn Iterator<Item = [usize; 3]> + 'a> {
    let n = priority.len();
    if n < 3 {
        return Box::new(std::iter::empty());
    }
    let seed = seed as usize % n;
    let mut rank = vec![0usize; n];
    for (r, &at) in priority.iter().enumerate() {
        rank[at] = r;
    }
    let rank = Rc::new(rank);
    let walk = priority.iter().enumerate().flat_map(move |(r, &p)| {
        let rank = rank.clone();
        let as_first = (0..n).map(move |j| (p, j));
        let as_second = (0..n).map(move |i| (i, p));
        // `k == p` exactly when `i + j + 1 + seed == p (mod n)`.
        let as_third = (0..n).map(move |i| (i, (p + 2 * n - 1 - seed - i) % n));
        // The three elements are distinct, so `p` holds exactly one slot of any
        // triple it is in, and each triple is reached once, from that slot.
        as_first
            .chain(as_second)
            .chain(as_third)
            .map(move |(i, j)| [i, j, (i + j + 1 + seed) % n])
            .filter(move |&[i, j, k]| {
                i != j && j != k && i != k && rank[i] >= r && rank[j] >= r && rank[k] >= r
            })
    });
    // Only a triple that can run counts toward the cap. Oversized ones are still
    // yielded, for the caller to count: they head the queue when the last-supplied
    // and largest states are the big ones, and spending the cap on them would leave
    // the law with no case. So the walk ends at the cap or at the last reachable
    // triple, `(n - 1)(n - 2)` of them, whichever comes first.
    Box::new(walk.scan(0usize, move |within, triple| {
        if *within >= target {
            return None;
        }
        if fits(&triple) {
            *within += 1;
        }
        Some(triple)
    }))
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
/// Strided rather than truncated, for the same reason the state sample strides:
/// observations usually arrive in time order, so the first N steps are all from the
/// same few minutes and would test a contract only against its own recent past. The
/// last supplied step is always included.
fn sampled_transitions(corpus: &Corpus, config: &GeneratorConfig) -> Vec<(Bytes, Bytes)> {
    let n = corpus.transitions.len();
    let keep = n.checked_sub(1).map(|last| vec![last]).unwrap_or_default();
    sample_indices(n, config.max_transitions, &keep)
        .into_iter()
        .map(|i| corpus.transitions[i].clone())
        .collect()
}

fn delta_pairs(corpus: &Corpus, arity: usize) -> Vec<Vec<usize>> {
    if corpus.deltas.len() < arity {
        return Vec::new();
    }
    // Latest delta first, so the budget cuts the earliest (#6053). A pair stays in
    // the order its deltas were observed.
    match arity {
        1 => (0..corpus.deltas.len()).rev().map(|d| vec![d]).collect(),
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
            for j in (0..corpus.deltas.len()).rev() {
                let Some(base_j) = corpus.delta_base(j) else {
                    continue;
                };
                for i in (0..j).rev() {
                    if corpus.delta_base(i) == Some(base_j) {
                        pairs.push(vec![i, j]);
                    }
                }
            }
            pairs
        }
        _ => Vec::new(),
    }
}

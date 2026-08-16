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
#[derive(Debug, Default, Clone)]
pub struct Corpus {
    pub states: Vec<Bytes>,
    pub deltas: Vec<Bytes>,
    pub summaries: Vec<Bytes>,
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
        self.deltas = dedup(self.deltas);
        self.summaries = dedup(self.summaries);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
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
    pub seed: u64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            max_cases: 512,
            properties: ConformanceProperty::ALL.to_vec(),
            max_case_bytes: 4 * 1024 * 1024,
            max_states_paired: 24,
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

fn delta_pairs(corpus: &Corpus, arity: usize) -> Vec<Vec<Bytes>> {
    if corpus.deltas.len() < arity {
        return Vec::new();
    }
    match arity {
        1 => corpus.deltas.iter().map(|d| vec![d.clone()]).collect(),
        2 => {
            let mut pairs = Vec::new();
            for (i, a) in corpus.deltas.iter().enumerate() {
                for b in corpus.deltas.iter().skip(i + 1) {
                    pairs.push(vec![a.clone(), b.clone()]);
                }
            }
            pairs
        }
        _ => Vec::new(),
    }
}

//! Bounded, restart-safe sampling of the states a peer naturally observes.
//!
//! A peer must not fuzz the contracts it hosts. It watches a small rotating set of
//! *focus* contracts, keeps a bounded and deliberately diverse sample of the states
//! that pass through anyway, and runs cheap checks over that sample. Across the
//! network many peers collectively cover many contracts while each one does almost
//! no extra work.
//!
//! Two things make the sampling non-trivial:
//!
//! **Core cannot see semantic diversity.** State is opaque bytes, so there is no
//! honest way to ask for "different kinds of state". Instead the sampler keeps
//! several cheap strata — earliest, recent, a uniform reservoir, largest, smallest —
//! whose behaviour is understandable without pretending to understand the
//! contract's data model. Deliberately absent: any byte-distance metric dressed up
//! as semantic distance.
//!
//! **The pathological contracts are the least diverse.** A contract stuck in an
//! oscillation offers the same two states forever. Deduplicating by content hash is
//! what stops those two states from consuming the entire budget, and it is why the
//! budget is counted in bytes rather than items: a handful of very large states must
//! not be able to blow up memory either.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::bundle::{ReplayBundle, Transition};
use super::generator::Corpus;
use super::verifier::Bytes;

/// Content hash of a canonical state, used as its identity everywhere in the store.
pub type StateHash = [u8; 32];

fn hash_of(bytes: &[u8]) -> StateHash {
    *blake3::hash(bytes).as_bytes()
}

/// Which stratum a sample was retained for. A state can belong to several; its bytes
/// are still stored once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Stratum {
    Earliest,
    Recent,
    Reservoir,
    Largest,
    Smallest,
}

impl Stratum {
    pub const ALL: &'static [Stratum] = &[
        Stratum::Earliest,
        Stratum::Recent,
        Stratum::Reservoir,
        Stratum::Largest,
        Stratum::Smallest,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Stratum::Earliest => "earliest",
            Stratum::Recent => "recent",
            Stratum::Reservoir => "reservoir",
            Stratum::Largest => "largest",
            Stratum::Smallest => "smallest",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplerConfig {
    pub earliest: usize,
    pub recent: usize,
    pub reservoir: usize,
    pub largest: usize,
    pub smallest: usize,
    pub transitions: usize,
    /// Hard ceiling on stored bytes for one contract, counting each distinct blob
    /// once. This is the limit that actually matters: item counts say nothing about
    /// memory when one state is 8 MB.
    pub max_bytes: usize,
    /// Any single state at or above this is never admitted. Without it, one
    /// oversized state could consume the whole per-contract budget and evict every
    /// useful small sample to do it.
    pub max_state_bytes: usize,
    /// Fixes the reservoir's choices. Persisted, so a restart resumes the same
    /// sequence rather than restarting the sample with a fresh bias.
    pub seed: u64,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            earliest: 4,
            recent: 8,
            reservoir: 8,
            largest: 4,
            smallest: 4,
            transitions: 8,
            max_bytes: 4 * 1024 * 1024,
            max_state_bytes: 1024 * 1024,
            seed: 0,
        }
    }
}

/// One observed `base + update -> result` step, stored by hash so the state bytes
/// are shared with the strata rather than duplicated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionRecord {
    pub base: StateHash,
    pub result: StateHash,
    pub incoming_state: Option<StateHash>,
    pub delta: Option<Vec<u8>>,
    pub summary: Option<Vec<u8>>,
}

/// What happened to an offered observation. Returned so callers can meter the
/// sampler without reaching into it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// New bytes, now retained in at least one stratum.
    Stored,
    /// Already held. Costs nothing and consumes no budget — this is the case that
    /// keeps an oscillating contract from filling the store with two states.
    Duplicate,
    /// Above [`SamplerConfig::max_state_bytes`].
    TooLarge,
    /// Would exceed the byte budget and nothing lower-value could be freed.
    NoBudget,
    /// Retained nowhere: every stratum it could have joined is full of samples that
    /// rank higher, so keeping the bytes would buy nothing.
    NotSelected,
}

/// The per-contract sample store.
///
/// Serializable in full, because a useful sample accumulates over hours or days and
/// a restart that discarded it would mean a peer never accumulates enough diversity
/// to find anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractSampler {
    config: SamplerConfig,
    /// Content-addressed blob store. Every stratum and transition refers to entries
    /// here by hash, so overlap costs nothing.
    blobs: BTreeMap<StateHash, Vec<u8>>,
    earliest: Vec<StateHash>,
    recent: Vec<StateHash>,
    reservoir: Vec<StateHash>,
    largest: Vec<StateHash>,
    smallest: Vec<StateHash>,
    transitions: VecDeque<TransitionRecord>,
    /// Count of DISTINCT states offered, which is what the reservoir must be uniform
    /// over. Counting every observation instead would let a contract that repeats one
    /// state a million times drown out everything else in the reservoir's arithmetic.
    distinct_seen: u64,
    /// Every observation offered, distinct or not. Telemetry only.
    total_seen: u64,
}

impl ContractSampler {
    pub fn new(config: SamplerConfig) -> Self {
        Self {
            config,
            blobs: BTreeMap::new(),
            earliest: Vec::new(),
            recent: Vec::new(),
            reservoir: Vec::new(),
            largest: Vec::new(),
            smallest: Vec::new(),
            transitions: VecDeque::new(),
            distinct_seen: 0,
            total_seen: 0,
        }
    }

    pub fn config(&self) -> &SamplerConfig {
        &self.config
    }

    /// Bytes actually held, counting each distinct blob once.
    ///
    /// Includes transition payloads. Deltas and summaries are contract-controlled
    /// buffers stored inline rather than content-addressed, so leaving them out of
    /// the accounting let a contract with large deltas hold far more than the
    /// advertised ceiling while every number here still looked healthy — a budget
    /// that measures only part of what it retains is not a budget.
    pub fn stored_bytes(&self) -> usize {
        self.blobs.values().map(Vec::len).sum::<usize>() + self.transition_payload_bytes()
    }

    fn transition_payload_bytes(&self) -> usize {
        self.transitions
            .iter()
            .map(|t| t.delta.as_ref().map_or(0, Vec::len) + t.summary.as_ref().map_or(0, Vec::len))
            .sum()
    }

    pub fn distinct_states(&self) -> usize {
        self.blobs.len()
    }

    pub fn distinct_seen(&self) -> u64 {
        self.distinct_seen
    }

    pub fn total_seen(&self) -> u64 {
        self.total_seen
    }

    pub fn members(&self, stratum: Stratum) -> &[StateHash] {
        match stratum {
            Stratum::Earliest => &self.earliest,
            Stratum::Reservoir => &self.reservoir,
            Stratum::Largest => &self.largest,
            Stratum::Smallest => &self.smallest,
            Stratum::Recent => &self.recent,
        }
    }

    /// Offer a state the peer has observed.
    pub fn observe_state(&mut self, state: &[u8]) -> Admission {
        self.total_seen += 1;
        if state.len() > self.config.max_state_bytes {
            return Admission::TooLarge;
        }
        let hash = hash_of(state);
        if self.blobs.contains_key(&hash) {
            // Refresh recency without spending budget: an oscillating contract
            // re-offering the same state should keep it current, not re-store it.
            self.touch_recent(hash);
            return Admission::Duplicate;
        }

        self.distinct_seen += 1;
        let wanted = self.strata_for(state.len(), hash);
        if wanted.is_empty() {
            return Admission::NotSelected;
        }
        if !self.make_room_for(state.len()) {
            return Admission::NoBudget;
        }

        self.blobs.insert(hash, state.to_vec());
        for stratum in wanted {
            self.admit_to(stratum, hash, state.len());
        }
        self.collect_garbage();
        Admission::Stored
    }

    /// Offer a `base + update -> result` observation.
    ///
    /// Transitions are worth more than loose states because they carry the context a
    /// delta needs in order to be replayable at all.
    pub fn observe_transition(
        &mut self,
        base: &[u8],
        incoming_state: Option<&[u8]>,
        delta: Option<&[u8]>,
        summary: Option<&[u8]>,
        result: &[u8],
    ) -> Admission {
        let base_admission = self.observe_state(base);
        let result_admission = self.observe_state(result);
        let incoming_admission = incoming_state.map(|incoming| self.observe_state(incoming));

        // Only record the step if EVERY state it references survived admission.
        //
        // The incoming state counts. Ignoring its result meant a transition could be
        // recorded whose `Some(incoming_state)` had been refused or evicted, and
        // `materialize` then silently turned that into `None` on export — so the
        // bundle claimed a transition while omitting the full-state update that
        // caused it, and anyone replaying it would be reasoning about a step that
        // never happened as described.
        let kept = |a: Admission| matches!(a, Admission::Stored | Admission::Duplicate);
        if !kept(base_admission)
            || !kept(result_admission)
            || incoming_admission.is_some_and(|a| !kept(a))
        {
            return Admission::NotSelected;
        }

        // Deltas and summaries are contract-controlled and stored inline, so they
        // get the same per-item ceiling as a state. A contract emitting a huge delta
        // must not be able to buy unbounded retention with it.
        let payload_bytes = delta.map_or(0, <[u8]>::len) + summary.map_or(0, <[u8]>::len);
        if payload_bytes > self.config.max_state_bytes {
            return Admission::TooLarge;
        }

        let record = TransitionRecord {
            base: hash_of(base),
            result: hash_of(result),
            incoming_state: incoming_state.map(hash_of),
            delta: delta.map(<[u8]>::to_vec),
            summary: summary.map(<[u8]>::to_vec),
        };
        if self.transitions.contains(&record) {
            return Admission::Duplicate;
        }
        self.transitions.push_back(record);
        while self.transitions.len() > self.config.transitions {
            self.transitions.pop_front();
        }
        // Transition payloads count against the byte budget, so admitting one can
        // put the store over it. Shed until it fits, exactly as a state would.
        while self.stored_bytes() > self.config.max_bytes {
            if !self.drop_lowest_value() {
                break;
            }
        }
        self.collect_garbage();
        Admission::Stored
    }

    /// Flatten into material the generator can build cases from.
    ///
    /// Strata are interleaved rather than concatenated, so that the generator's own
    /// pairing — which walks the list in order — naturally crosses strata (earliest
    /// against recent, reservoir against largest) instead of only pairing states
    /// that were adjacent in time.
    pub fn corpus(&self) -> Corpus {
        let mut ordered: Vec<StateHash> = Vec::new();
        let mut round = 0usize;
        loop {
            let mut produced = false;
            for stratum in Stratum::ALL {
                let members = self.members(*stratum);
                if let Some(hash) = members.get(round) {
                    produced = true;
                    if !ordered.contains(hash) {
                        ordered.push(*hash);
                    }
                }
            }
            if !produced {
                break;
            }
            round += 1;
        }

        let states: Vec<Bytes> = ordered
            .iter()
            .filter_map(|h| self.blobs.get(h))
            .map(|b| Arc::from(b.as_slice()))
            .collect();
        let deltas: Vec<Bytes> = self
            .transitions
            .iter()
            .filter_map(|t| t.delta.as_ref())
            .map(|d| Arc::from(d.as_slice()))
            .collect();
        let summaries: Vec<Bytes> = self
            .transitions
            .iter()
            .filter_map(|t| t.summary.as_ref())
            .map(|s| Arc::from(s.as_slice()))
            .collect();

        Corpus {
            states,
            deltas,
            summaries,
            ..Default::default()
        }
        .deduplicated()
    }

    /// Export as a portable replay bundle for offline analysis.
    ///
    /// `code_hash` identifies the WASM this corpus was observed against, and must be
    /// supplied when `code` is not embedded. Defaulting it to zeroes (the previous
    /// behaviour) produced a bundle that named no contract at all, so replaying it
    /// against an arbitrary WASM would silently "check" a corpus that was never
    /// produced by that contract — findings and clean runs alike would be
    /// meaningless.
    pub fn to_bundle(
        &self,
        code: Option<Vec<u8>>,
        code_hash: Option<[u8; 32]>,
        parameters: Vec<u8>,
    ) -> ReplayBundle {
        let code_hash = code
            .as_ref()
            .map(|c| *blake3::hash(c).as_bytes())
            .or(code_hash);
        let corpus = self.corpus();
        ReplayBundle {
            schema_version: super::bundle::BUNDLE_SCHEMA_VERSION,
            code,
            code_hash,
            parameters,
            instance: None,
            states: corpus.states.iter().map(|s| s.to_vec()).collect(),
            deltas: corpus.deltas.iter().map(|d| d.to_vec()).collect(),
            summaries: corpus.summaries.iter().map(|s| s.to_vec()).collect(),
            transitions: self
                .transitions
                .iter()
                .filter_map(|t| self.materialize(t))
                .collect(),
            related: Vec::new(),
            note: None,
        }
    }

    fn materialize(&self, record: &TransitionRecord) -> Option<Transition> {
        Some(Transition {
            base_state: self.blobs.get(&record.base)?.clone(),
            result_state: self.blobs.get(&record.result)?.clone(),
            incoming_state: record
                .incoming_state
                .and_then(|h| self.blobs.get(&h).cloned()),
            delta: record.delta.clone(),
            summary: record.summary.clone(),
        })
    }

    /// Move a re-observed state to the newest end of the recent stratum.
    ///
    /// A state that aged out of `recent` while staying retained by another stratum
    /// must be re-inserted, not ignored: otherwise "recent" stops meaning "most
    /// recently observed" for exactly the states the peer keeps seeing, which is the
    /// population that matters most.
    fn touch_recent(&mut self, hash: StateHash) {
        if self.config.recent == 0 {
            return;
        }
        if let Some(pos) = self.recent.iter().position(|h| *h == hash) {
            self.recent.remove(pos);
        }
        self.recent.push(hash);
        while self.recent.len() > self.config.recent {
            self.recent.remove(0);
        }
    }

    /// Decide which strata a newly-seen state belongs to, without mutating anything.
    fn strata_for(&self, len: usize, hash: StateHash) -> Vec<Stratum> {
        let mut wanted = Vec::new();
        if self.earliest.len() < self.config.earliest {
            wanted.push(Stratum::Earliest);
        }
        if self.config.recent > 0 {
            // Recency always admits; it evicts its own oldest member to make space.
            wanted.push(Stratum::Recent);
        }
        if self.reservoir_admits(hash) {
            wanted.push(Stratum::Reservoir);
        }
        if self.largest.len() < self.config.largest
            || self
                .largest
                .last()
                .and_then(|h| self.blobs.get(h))
                .is_some_and(|b| b.len() < len)
        {
            wanted.push(Stratum::Largest);
        }
        if self.smallest.len() < self.config.smallest
            || self
                .smallest
                .last()
                .and_then(|h| self.blobs.get(h))
                .is_some_and(|b| b.len() > len)
        {
            wanted.push(Stratum::Smallest);
        }
        wanted
    }

    /// Algorithm R, with the coin flip derived from the seed and the observation
    /// index instead of from an RNG.
    ///
    /// Derived rather than drawn so that the same observation sequence produces the
    /// same sample on any peer and across a restart. A reservoir that reseeded itself
    /// on restart would quietly bias toward whatever happened after the last reboot.
    fn reservoir_admits(&self, _hash: StateHash) -> bool {
        if self.config.reservoir == 0 {
            return false;
        }
        if self.reservoir.len() < self.config.reservoir {
            return true;
        }
        let n = self.distinct_seen.max(1);
        splitmix64(self.config.seed ^ n) % n < self.config.reservoir as u64
    }

    fn admit_to(&mut self, stratum: Stratum, hash: StateHash, len: usize) {
        match stratum {
            Stratum::Earliest => self.earliest.push(hash),
            Stratum::Recent => {
                self.recent.push(hash);
                while self.recent.len() > self.config.recent {
                    self.recent.remove(0);
                }
            }
            Stratum::Reservoir => {
                if self.reservoir.len() < self.config.reservoir {
                    self.reservoir.push(hash);
                } else {
                    let n = self.distinct_seen.max(1);
                    let victim = (splitmix64(self.config.seed ^ n.wrapping_mul(3)) as usize)
                        % self.reservoir.len();
                    self.reservoir[victim] = hash;
                }
            }
            Stratum::Largest => {
                self.largest.push(hash);
                let blobs = &self.blobs;
                self.largest
                    .sort_by_key(|h| std::cmp::Reverse(blobs.get(h).map_or(0, Vec::len)));
                self.largest.truncate(self.config.largest);
            }
            Stratum::Smallest => {
                self.smallest.push(hash);
                let blobs = &self.blobs;
                self.smallest
                    .sort_by_key(|h| blobs.get(h).map_or(usize::MAX, Vec::len));
                self.smallest.truncate(self.config.smallest);
            }
        }
        let _ = len;
    }

    /// Free space for `incoming` bytes, or report that we cannot.
    ///
    /// Eviction follows the sampling policy rather than a generic LRU: the recent
    /// stratum is the one designed to churn, so it gives ground first, then
    /// transitions, then the reservoir. The size-ranked and earliest strata give
    /// ground last, because they hold what nothing else preserves.
    ///
    /// But they DO give ground. An earlier version exempted `earliest`, `largest` and
    /// `smallest` entirely, which wedged the store permanently under its own default
    /// config: with `max_state_bytes` at 1 MiB and `max_bytes` at 4 MiB, a contract
    /// whose first few observed states are each near 1 MiB fills those three strata
    /// with blobs nothing may evict, and every later admission — of any size — then
    /// fails forever, because the only evictable pools hold nothing but duplicate
    /// references to the same pinned blobs. The store would stop learning, silently,
    /// with no error, for exactly the large-state contracts this mechanism exists to
    /// examine. Each stratum keeps at least one member so none is emptied outright.
    fn make_room_for(&mut self, incoming: usize) -> bool {
        if incoming > self.config.max_bytes {
            return false;
        }
        while self.stored_bytes() + incoming > self.config.max_bytes {
            let freed = self.drop_lowest_value();
            if !freed {
                return false;
            }
        }
        true
    }

    fn drop_lowest_value(&mut self) -> bool {
        if self.collect_garbage() {
            return true;
        }
        // Order of surrender: recency churns by design, then transition history,
        // then the reservoir. Each keeps at least one member so a stratum is never
        // emptied outright — a stratum that has been silently emptied still reports
        // as present and would make coverage telemetry lie.
        if self.recent.len() > 1 {
            self.recent.remove(0);
            self.collect_garbage();
            return true;
        }
        if self.transitions.pop_front().is_some() {
            self.collect_garbage();
            return true;
        }
        if self.reservoir.len() > 1 {
            self.reservoir.pop();
            self.collect_garbage();
            return true;
        }
        // Last resort: the size-ranked strata, largest first, since one huge blob is
        // what usually caused the pressure. Without this the store can wedge forever
        // (see the note on `make_room_for`).
        if self.largest.len() > 1 {
            self.largest.pop();
            self.collect_garbage();
            return true;
        }
        if self.smallest.len() > 1 {
            self.smallest.pop();
            self.collect_garbage();
            return true;
        }
        // And finally the earliest stratum, which is the most valuable and so goes
        // last. Dropping its newest member keeps the oldest observation, which is the
        // one nothing else in the store preserves.
        if self.earliest.len() > 1 {
            self.earliest.pop();
            self.collect_garbage();
            return true;
        }
        false
    }

    /// Drop blobs no stratum and no transition refers to. Returns whether anything
    /// was freed.
    fn collect_garbage(&mut self) -> bool {
        let mut referenced: HashMap<StateHash, ()> = HashMap::new();
        for stratum in Stratum::ALL {
            for hash in self.members(*stratum) {
                referenced.insert(*hash, ());
            }
        }
        for hash in &self.recent {
            referenced.insert(*hash, ());
        }
        for transition in &self.transitions {
            referenced.insert(transition.base, ());
            referenced.insert(transition.result, ());
            if let Some(incoming) = transition.incoming_state {
                referenced.insert(incoming, ());
            }
        }
        let before = self.blobs.len();
        self.blobs.retain(|hash, _| referenced.contains_key(hash));
        self.blobs.len() < before
    }
}

/// A cheap, well-distributed mixing function. Used instead of an RNG so sampling
/// decisions are a pure function of `(seed, observation index)`.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

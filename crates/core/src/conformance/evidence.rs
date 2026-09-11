//! Evidence: a bounded, self-contained reproducer.
//!
//! The design rule the RFC insists on is that peers propagate *evidence*, never a
//! verdict. A receiving peer does not trust that the sender found a violation; it
//! re-executes the case against its own copy of the contract with its own runtime
//! and reaches its own conclusion. That is what makes the mechanism safe without
//! any distributed trust: the case spreads, every deletion decision stays local.
//!
//! Consequently everything needed to re-run the check must be *in* the evidence,
//! and the whole thing must stay small enough that verifying one is cheaper than
//! being asked to.
//!
//! The sharper form of that first requirement is what [`ConformanceEvidence::check_bounds`]
//! enforces first: a law whose PREMISE cannot be carried in the bytes is not
//! shippable at all, however small it is. Re-execution re-establishes a universally
//! quantified identity over valid states no matter where the states came from, but
//! it cannot re-establish a fact about how the sender OBSERVED them - and the sender
//! is precisely the party this design refuses to trust. Such a property is marked
//! [`PremiseSource::LocalProvenance`] and refused at the door.
//!
//! That check is not advisory. [`ConformanceEvidence::to_case`] - the only way to
//! turn evidence into something runnable - performs it itself and returns a
//! `Result`, so there is no path from a byte string to the WASM that skips it. A doc
//! comment saying "call `check_bounds` first" would be the only thing standing
//! between a future receive path and the runtime, and a doc comment is not a gate.

use std::collections::HashMap;
use std::sync::Arc;

use freenet_stdlib::prelude::{ContractInstanceId, RelatedContracts, State};
use serde::{Deserialize, Serialize};

use super::property::{ConformanceProperty, PremiseSource, Violation};
use super::verifier::ConformanceCase;

/// Bump when the meaning of a field changes. A peer that does not understand a
/// schema version rejects the evidence rather than guessing: misinterpreting a
/// reproducer is exactly how a false positive would spread.
pub const EVIDENCE_SCHEMA_VERSION: u16 = 2;

/// Magic prefix so a truncated, raw, or unrelated file fails fast with a clear message
/// rather than as a confusing deserialization error deep in bincode.
pub const EVIDENCE_MAGIC: &[u8; 8] = b"FRNTEVD1";

/// Errors encountered when encoding or decoding conformance evidence.
#[derive(Debug, thiserror::Error)]
pub enum EvidenceError {
    #[error("not conformance evidence (bad magic)")]
    BadMagic,
    #[error(
        "evidence file is truncated: magic matched but the 10-byte framing header is incomplete \
         ({len} byte(s)); the file was likely cut off mid-write and should be regenerated"
    )]
    Truncated { len: usize },
    #[error("evidence uses schema version {found}, this build understands {supported}")]
    UnsupportedSchema { found: u16, supported: u16 },
    #[error(
        "evidence framing header specifies schema {header}, but deserialized payload specifies {body}"
    )]
    MismatchedBodySchema { header: u16, body: u16 },
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

/// Hard ceiling on one evidence object's input bytes.
///
/// Chosen so that verifying evidence is unambiguously cheaper than the update
/// traffic a non-converging contract already generates, and so an attacker cannot
/// use evidence as an amplification vector. Operational tunable, not a protocol
/// constant.
pub const MAX_EVIDENCE_INPUT_BYTES: usize = 512 * 1024;

/// Hard ceiling on how many related-contract states one evidence object may carry.
pub const MAX_EVIDENCE_RELATED: usize = 8;

/// A content hash identifying one reproducer, used for deduplication so a peer
/// neither re-verifies nor re-forwards a case it has already seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EvidenceId([u8; 32]);

impl EvidenceId {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for EvidenceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(&self.0[..8]))
    }
}

/// Which runtime produced a finding.
///
/// Two peers on different core versions can legitimately disagree about a
/// contract's behaviour, and that disagreement is a fact worth recording rather
/// than a violation to act on. Shadow-mode telemetry reports it explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub core_version: String,
    pub evidence_schema: u16,
}

impl RuntimeIdentity {
    pub fn current() -> Self {
        Self {
            core_version: env!("CARGO_PKG_VERSION").to_string(),
            evidence_schema: EVIDENCE_SCHEMA_VERSION,
        }
    }
}

/// Why a peer refused to even look at a piece of evidence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvidenceRejected {
    #[error("evidence schema {found} is not supported (this peer speaks {supported})")]
    UnsupportedSchema { found: u16, supported: u16 },
    #[error("evidence carries {found} input bytes, limit is {limit}")]
    TooLarge { found: usize, limit: usize },
    #[error("evidence carries {found} related contracts, limit is {limit}")]
    TooManyRelated { found: usize, limit: usize },
    /// The property is not self-verifying, so no amount of re-execution could
    /// establish its premise. See [`PremiseSource`].
    #[error(
        "{property} rests on provenance the evidence bytes cannot carry, so it is \
         local-only and never shippable as evidence"
    )]
    NotSelfVerifying { property: ConformanceProperty },
    #[error(
        "{property} needs {want} states and {want_deltas} deltas, evidence has {got} and {got_deltas}"
    )]
    Arity {
        property: ConformanceProperty,
        want: usize,
        got: usize,
        want_deltas: usize,
        got_deltas: usize,
    },
}

/// A self-contained reproducer for one conformance property against one contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceEvidence {
    pub schema_version: u16,
    /// The contract instance the case is about. Parameters are carried separately
    /// because the same code with different parameters is a different instance and
    /// must not inherit another instance's findings.
    pub contract: ContractInstanceId,
    pub parameters: Vec<u8>,
    pub property: ConformanceProperty,
    pub states: Vec<Vec<u8>>,
    pub deltas: Vec<Vec<u8>>,
    pub summary: Option<Vec<u8>>,
    pub related: Vec<(ContractInstanceId, Vec<u8>)>,
    /// What the discovering peer saw. Diagnostics only — recipients recompute.
    pub observed: Option<Violation>,
    pub runtime: RuntimeIdentity,
}

impl ConformanceEvidence {
    /// Build evidence from the case that produced a finding.
    pub fn new(
        contract: ContractInstanceId,
        parameters: Vec<u8>,
        case: &ConformanceCase,
        observed: Option<Violation>,
    ) -> Self {
        Self {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            contract,
            parameters,
            property: case.property,
            states: case.states.iter().map(|s| s.to_vec()).collect(),
            deltas: case.deltas.iter().map(|d| d.to_vec()).collect(),
            summary: case.summary.as_ref().map(|s| s.to_vec()),
            related: related_to_pairs(&case.related),
            observed,
            runtime: RuntimeIdentity::current(),
        }
    }

    pub fn input_bytes(&self) -> usize {
        self.states.iter().map(Vec::len).sum::<usize>()
            + self.deltas.iter().map(Vec::len).sum::<usize>()
            + self.summary.as_ref().map_or(0, Vec::len)
            + self.related.iter().map(|(_, s)| s.len()).sum::<usize>()
            + self.parameters.len()
    }

    /// Reject anything malformed, oversized or unsupported *before* spending any
    /// WASM fuel on it. This is the front door of the untrusted path.
    pub fn check_bounds(&self) -> Result<(), EvidenceRejected> {
        if self.schema_version != EVIDENCE_SCHEMA_VERSION {
            return Err(EvidenceRejected::UnsupportedSchema {
                found: self.schema_version,
                supported: EVIDENCE_SCHEMA_VERSION,
            });
        }
        // Refuse a property whose premise the recipient could not re-establish even
        // by running every byte of this evidence through its own copy of the
        // contract.
        //
        // This is the front door's most important check, and it is not a size or a
        // schema question: it is the one place the ship-inputs-not-verdicts design
        // can be subverted. Every other property here is a universally quantified
        // identity over valid states, so re-execution re-establishes the whole
        // premise and a fabricated case can only surface a real defect sooner. A
        // property that is a law only because the SENDER observed something hands
        // the recipient an accusation it can confirm but cannot check, because the
        // witness is not in the bytes and cannot be put there.
        //
        // Two properties are in that position, for the same reason applied to
        // different provenance. `TransitionPathAgreement` needs the witness that
        // `result` was reached from `base`; a fabricated pair from a conforming
        // grow-only contract is structurally indistinguishable from a genuine
        // information-losing update. `DeltaPermutationInvariance` needs the witness
        // that both deltas were observed against the SAME base; a causally-sequenced
        // pair permutes to two different states on contracts that are perfectly
        // sound, because production would never apply them in the other order.
        // Either way, every peer receiving the fabrication would independently reach
        // a removal-eligible verdict against a correct contract.
        //
        // Refused rather than deprioritised: a lower rank still lets it in, and the
        // whole point of the front door is that unsound input never reaches the
        // WASM. The property keeps its full value where provenance is observed
        // directly (shadow mode, `fdev`); it simply never travels.
        if !self.property.is_self_verifying() {
            debug_assert_eq!(
                self.property.premise_source(),
                PremiseSource::LocalProvenance
            );
            return Err(EvidenceRejected::NotSelfVerifying {
                property: self.property,
            });
        }
        let bytes = self.input_bytes();
        if bytes > MAX_EVIDENCE_INPUT_BYTES {
            return Err(EvidenceRejected::TooLarge {
                found: bytes,
                limit: MAX_EVIDENCE_INPUT_BYTES,
            });
        }
        if self.related.len() > MAX_EVIDENCE_RELATED {
            return Err(EvidenceRejected::TooManyRelated {
                found: self.related.len(),
                limit: MAX_EVIDENCE_RELATED,
            });
        }
        // EXACT arity, not "at least".
        //
        // A minimum-only check lets evidence carry arbitrarily many trailing states.
        // Empty vectors weigh nothing against the byte budget, so they slip past it,
        // yet `verify_case` validates every supplied state through WASM — so a
        // sender could buy unbounded execution for free. It also breaks
        // deduplication, since varying the padding varies the id while the finding
        // stays the same.
        let want = self.property.state_arity();
        let want_deltas = self.property.delta_arity();
        if self.states.len() != want || self.deltas.len() != want_deltas {
            return Err(EvidenceRejected::Arity {
                property: self.property,
                want,
                got: self.states.len(),
                want_deltas,
                got_deltas: self.deltas.len(),
            });
        }
        // `summary` deliberately gets neither an arity nor a nullity constraint, and
        // the reason is that the two hazards the exact-arity check closes do not both
        // reach it.
        //
        // It is one `Option`, not a `Vec`, so "arbitrarily many trailing entries" has
        // no analogue. Its bytes DO count in `input_bytes`, so unlike an empty
        // trailing state — which weighs nothing and is therefore free — padding here
        // is paid for against `MAX_EVIDENCE_INPUT_BYTES`. And it buys no execution:
        // `DeltaDeterminism` is the only property that reads it, and a supplied
        // summary REPLACES a `summarize_state` call the verifier would otherwise
        // make, so it removes WASM work rather than adding it. Every other property
        // ignores the field entirely.
        //
        // What does reach it is the deduplication half: `id()` hashes the summary,
        // so toggling `Some`/`None` or varying its bytes yields a distinct id for the
        // same underlying finding. That is a real residual, and it is left open
        // because it costs the sender bandwidth per copy where the arity padding cost
        // nothing — a rate, not a hole. Two things should change the answer: a second
        // property starting to consume `summary`, or evidence acquiring a gossip
        // receive path where dedup carries weight against a hostile sender. Either
        // one makes "must be `None` unless the property consumes it" worth its own
        // rejection variant.
        Ok(())
    }

    /// Content hash over the inputs that determine the outcome.
    ///
    /// Deliberately excludes [`Self::observed`] and [`Self::runtime`]: two peers that
    /// independently discover the same defect must produce the same id, otherwise
    /// deduplication fails open and the same case circulates once per discoverer.
    pub fn id(&self) -> EvidenceId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"freenet-conformance-evidence-v1");
        hasher.update(&self.schema_version.to_le_bytes());
        hasher.update(self.contract.as_bytes());
        hash_blob(&mut hasher, &self.parameters);
        hasher.update(self.property.as_str().as_bytes());
        hasher.update(&(self.states.len() as u64).to_le_bytes());
        for state in &self.states {
            hash_blob(&mut hasher, state);
        }
        hasher.update(&(self.deltas.len() as u64).to_le_bytes());
        for delta in &self.deltas {
            hash_blob(&mut hasher, delta);
        }
        match &self.summary {
            Some(summary) => {
                hasher.update(&[1u8]);
                hash_blob(&mut hasher, summary);
            }
            None => {
                hasher.update(&[0u8]);
            }
        }
        // Related state is sorted so two peers holding the same map in different
        // iteration orders still agree on the id.
        let mut related = self.related.clone();
        related.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        hasher.update(&(related.len() as u64).to_le_bytes());
        for (id, state) in &related {
            hasher.update(id.as_bytes());
            hash_blob(&mut hasher, state);
        }
        EvidenceId(*hasher.finalize().as_bytes())
    }

    /// Rebuild the runnable case, refusing anything [`Self::check_bounds`] rejects.
    ///
    /// The check is folded in here rather than left to the caller on purpose. This is
    /// the only way to turn an evidence object into something the WASM runtime will
    /// execute, so making it the enforcement point means no caller — including a
    /// future gossip receive path that does not exist yet — can reach the runtime
    /// with unbounded, wrong-arity, or non-self-verifying input. The previous shape
    /// was a `pub fn` returning the case unconditionally with a "call `check_bounds`
    /// first" doc comment, which is a convention rather than a gate, and conventions
    /// are what the untrusted front door cannot be built out of.
    ///
    /// # Errors
    ///
    /// Returns whatever [`Self::check_bounds`] rejects, and nothing else — the
    /// rebuild itself cannot fail. So an [`EvidenceRejected`] here means the evidence
    /// carries an unsupported [`EVIDENCE_SCHEMA_VERSION`], rests on provenance the
    /// bytes cannot carry ([`EvidenceRejected::NotSelfVerifying`]), exceeds
    /// [`MAX_EVIDENCE_INPUT_BYTES`] or [`MAX_EVIDENCE_RELATED`], or does not carry
    /// exactly the state and delta counts its property requires.
    ///
    /// Note for callers upgrading past the signature change: this returned
    /// `ConformanceCase` directly until the gate moved inside, so a caller that
    /// previously ignored `check_bounds` now gets the refusal it was skipping.
    pub fn to_case(&self) -> Result<ConformanceCase, EvidenceRejected> {
        self.check_bounds()?;
        let related: HashMap<ContractInstanceId, Option<State<'static>>> = self
            .related
            .iter()
            .map(|(id, state)| (*id, Some(State::from(state.clone()))))
            .collect();
        let related = RelatedContracts::from(related);
        Ok(ConformanceCase {
            property: self.property,
            states: self
                .states
                .iter()
                .map(|s| Arc::from(s.as_slice()))
                .collect(),
            deltas: self
                .deltas
                .iter()
                .map(|d| Arc::from(d.as_slice()))
                .collect(),
            summary: self.summary.as_ref().map(|s| Arc::from(s.as_slice())),
            related,
        })
    }

    /// Encode with framing: 8-byte magic (`FRNTEVD1`), 2-byte schema version (LE),
    /// followed by the bincode-serialized payload.
    pub fn encode(&self) -> Result<Vec<u8>, EvidenceError> {
        let mut out = Vec::with_capacity(self.input_bytes() + 128);
        out.extend_from_slice(EVIDENCE_MAGIC);
        out.extend_from_slice(&self.schema_version.to_le_bytes());
        let body = bincode::serialize(self).map_err(|e| EvidenceError::Encode(e.to_string()))?;
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Decode an evidence file, verifying magic and schema version *before*
    /// attempting bincode deserialization.
    ///
    /// The check order matters:
    /// 1. Magic first — so a truncated framed file (`b"FRNTEVD1"` with nothing after)
    ///    reports [`EvidenceError::Truncated`] rather than [`EvidenceError::BadMagic`].
    ///    A disk that fills mid-write produces a partial file; telling the user "not
    ///    conformance evidence" sends them hunting for the wrong problem.
    /// 2. Length second — once the magic matches, a short header is a truncation.
    /// 3. Legacy sniff — if the magic did not match and the first two bytes look like
    ///    a schema version, try to decode the raw bincode payload (v0.2.129–v0.2.133
    ///    wrote evidence without the header). Schema 2 files from those releases are
    ///    byte-compatible with the current struct, so a successful decode is returned
    ///    rather than an error. Schema 1 is refused: the struct changed between 1 and 2.
    pub fn decode(bytes: &[u8]) -> Result<Self, EvidenceError> {
        const HEADER_LEN: usize = EVIDENCE_MAGIC.len() + 2;

        // Check magic FIRST so a truncated framed file is recognized as such rather
        // than misreported as "not conformance evidence".
        if bytes.starts_with(EVIDENCE_MAGIC) {
            if bytes.len() < HEADER_LEN {
                return Err(EvidenceError::Truncated { len: bytes.len() });
            }
            let version =
                u16::from_le_bytes([bytes[EVIDENCE_MAGIC.len()], bytes[EVIDENCE_MAGIC.len() + 1]]);
            if version != EVIDENCE_SCHEMA_VERSION {
                return Err(EvidenceError::UnsupportedSchema {
                    found: version,
                    supported: EVIDENCE_SCHEMA_VERSION,
                });
            }
            let evidence: Self = bincode::deserialize(&bytes[HEADER_LEN..])
                .map_err(|e| EvidenceError::Decode(e.to_string()))?;
            if evidence.schema_version != version {
                return Err(EvidenceError::MismatchedBodySchema {
                    header: version,
                    body: evidence.schema_version,
                });
            }
            return Ok(evidence);
        }

        // Legacy sniff: pre-0.2.134 builds wrote raw bincode without the framing header.
        // The first field is `schema_version: u16` in LE, so a real legacy file begins
        // `01 00` (schema 1) or `02 00` (schema 2).
        //
        // Schema 1 is refused: the struct layout changed when `settling` was added.
        // Schema 2 is byte-compatible with the current struct, so we attempt a direct
        // bincode decode and return it if it succeeds. Failure (e.g. a random file that
        // happens to start with `02 00`) falls through to BadMagic rather than exposing
        // a decode error for input that is almost certainly not evidence at all.
        if bytes.len() >= 2 {
            let legacy_version = u16::from_le_bytes([bytes[0], bytes[1]]);
            if legacy_version == 1 {
                return Err(EvidenceError::UnsupportedSchema {
                    found: 1,
                    supported: EVIDENCE_SCHEMA_VERSION,
                });
            }
            if legacy_version == 2 {
                if let Ok(evidence) = bincode::deserialize::<Self>(bytes) {
                    return Ok(evidence);
                }
                // Not a real legacy evidence file; fall through to BadMagic.
            }
        }

        Err(EvidenceError::BadMagic)
    }
}

/// Length-prefix each blob so `["ab", "c"]` and `["a", "bc"]` cannot collide.
fn hash_blob(hasher: &mut blake3::Hasher, blob: &[u8]) {
    hasher.update(&(blob.len() as u64).to_le_bytes());
    hasher.update(blob);
}

fn related_to_pairs(related: &RelatedContracts<'static>) -> Vec<(ContractInstanceId, Vec<u8>)> {
    let mut pairs: Vec<_> = related
        .states()
        .filter_map(|(id, state)| state.as_ref().map(|s| (*id, s.as_ref().to_vec())))
        .collect();
    pairs.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
    pairs
}

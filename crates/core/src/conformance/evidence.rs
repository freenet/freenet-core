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

use bincode::Options as _;
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
        "evidence is truncated: it ends after {len} byte(s), before it is complete; the \
         file was likely cut off mid-write and should be regenerated"
    )]
    Truncated { len: usize },
    #[error("evidence uses schema version {found}, this build understands {supported}")]
    UnsupportedSchema { found: u16, supported: u16 },
    /// An unframed file whose first two bytes read as a schema version this build
    /// cannot decode. Worded as "looks like" because any binary file can begin with
    /// the same two bytes.
    #[error(
        "this looks like unframed evidence written by an older build (schema {found}), \
         which this build cannot read; re-capture it with this build"
    )]
    LegacyUnsupported { found: u16 },
    /// An unframed file that starts like pre-framing schema-2 evidence but does not
    /// decode as it.
    #[error(
        "this starts like unframed schema-2 evidence from v0.2.133 but does not decode \
         as it ({0}); if it is evidence, the file is damaged or truncated"
    )]
    LegacyUndecodable(String),
    #[error(
        "evidence framing header specifies schema {header}, but deserialized payload specifies {body}"
    )]
    MismatchedBodySchema { header: u16, body: u16 },
    #[error("encode: {0}")]
    Encode(String),
    #[error(
        "evidence payload is {found} bytes, more than any evidence this build accepts \
         ({limit}); it was not decoded"
    )]
    PayloadTooLarge { found: usize, limit: usize },
    #[error("decode: {0}")]
    Decode(String),
}

/// Hard ceiling on one evidence object's input bytes: the states, deltas, summary,
/// related states and parameters that verifying it will execute.
///
/// Chosen so that verifying evidence is unambiguously cheaper than the update
/// traffic a non-converging contract already generates, and so an attacker cannot
/// use evidence as an amplification vector. Operational tunable, not a protocol
/// constant.
///
/// This counts execution inputs only. The two free-text fields are bounded
/// separately by [`MAX_EVIDENCE_TEXT_BYTES`], and the encoded object as a whole by
/// [`MAX_EVIDENCE_ENCODED_BYTES`] while it is decoded.
pub const MAX_EVIDENCE_INPUT_BYTES: usize = 512 * 1024;

/// Hard ceiling on how many related-contract states one evidence object may carry.
pub const MAX_EVIDENCE_RELATED: usize = 8;

/// Hard ceiling on each free-text field the sender writes: `observed.detail` and
/// `runtime.core_version`.
///
/// Neither is an execution input, so [`ConformanceEvidence::input_bytes`] does not
/// count them, and before #5581 they were the one part of an evidence object whose
/// size the sender chose freely. Both are diagnostics that a recipient never acts
/// on. A kilobyte is far more than this crate writes: its longest detail is a
/// sentence containing two byte counts.
pub const MAX_EVIDENCE_TEXT_BYTES: usize = 1024;

/// Hard ceiling on the encoded payload of one evidence object, enforced while
/// decoding. It measures the payload after the 10-byte header (8-byte magic, 2-byte
/// schema version), not the whole file.
///
/// [`ConformanceEvidence::check_bounds`] can only inspect an object that decoding
/// has already built, so a limit applied there arrives after the allocation it
/// exists to prevent. This one is applied before bincode parses anything. It is
/// sized to hold the largest object `check_bounds` accepts: [`MAX_EVIDENCE_INPUT_BYTES`]
/// of inputs, two [`MAX_EVIDENCE_TEXT_BYTES`] fields, and the fixed-size fields and
/// length prefixes around them, which come to a few kilobytes. The rest is slack.
/// `every_evidence_check_bounds_accepts_fits_the_decode_limit` pins that the two
/// limits agree.
pub const MAX_EVIDENCE_ENCODED_BYTES: usize = MAX_EVIDENCE_INPUT_BYTES + 16 * 1024;

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
    /// A sender-written text field is longer than [`MAX_EVIDENCE_TEXT_BYTES`].
    #[error("evidence field {field} is {found} bytes, limit is {limit}")]
    TextTooLong {
        field: &'static str,
        found: usize,
        limit: usize,
    },
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
            // Truncated so that evidence this crate writes always passes its own
            // `check_bounds`. fdev's writer skips evidence that fails it, so an
            // overlong detail would otherwise drop the finding without a word.
            observed: observed.map(|mut violation| {
                truncate_text(&mut violation.detail, MAX_EVIDENCE_TEXT_BYTES);
                violation
            }),
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
        // The two text fields are not execution inputs, so `input_bytes` does not
        // count them. Without this the sender alone decides their size (#5581).
        if let Some(observed) = &self.observed {
            check_text_len("observed.detail", &observed.detail)?;
        }
        check_text_len("runtime.core_version", &self.runtime.core_version)?;
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
    /// [`MAX_EVIDENCE_INPUT_BYTES`], [`MAX_EVIDENCE_RELATED`] or
    /// [`MAX_EVIDENCE_TEXT_BYTES`], or does not carry exactly the state and delta
    /// counts its property requires.
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
        // The header is written from this field, so any other value would produce a
        // file that `decode` then refuses as an unsupported schema.
        if self.schema_version != EVIDENCE_SCHEMA_VERSION {
            return Err(EvidenceError::Encode(format!(
                "schema_version is {}, but this build writes only schema {EVIDENCE_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        let mut out = Vec::with_capacity(self.input_bytes() + 128);
        out.extend_from_slice(EVIDENCE_MAGIC);
        out.extend_from_slice(&self.schema_version.to_le_bytes());
        let body = bincode::serialize(self).map_err(|e| EvidenceError::Encode(e.to_string()))?;
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Decode evidence from an untrusted source: framed evidence only.
    ///
    /// This is the decoder for anything a peer could have written. It accepts only
    /// the framed format (8-byte magic, 2-byte schema version, payload), so every
    /// input must match the full magic first. Files written before framing existed
    /// are read by [`Self::decode_file`], which is for files an operator points at
    /// and must never see bytes from a peer.
    ///
    /// The check order matters. Magic first, so a file cut short inside its header
    /// reports [`EvidenceError::Truncated`] rather than claiming it is not evidence:
    /// a disk that fills mid-write produces exactly that, and "not conformance
    /// evidence" sends its owner looking for the wrong problem. The payload then goes
    /// through [`decode_body`], which refuses one larger than
    /// [`MAX_EVIDENCE_ENCODED_BYTES`] or followed by trailing bytes, and a payload
    /// that ends early is reported as truncated as well.
    pub fn decode(bytes: &[u8]) -> Result<Self, EvidenceError> {
        const HEADER_LEN: usize = EVIDENCE_MAGIC.len() + 2;
        if !bytes.starts_with(EVIDENCE_MAGIC) {
            return Err(EvidenceError::BadMagic);
        }
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
        let evidence = decode_body(&bytes[HEADER_LEN..]).map_err(|error| match error {
            BodyError::TooLarge { found } => EvidenceError::PayloadTooLarge {
                found,
                limit: MAX_EVIDENCE_ENCODED_BYTES,
            },
            BodyError::EndedEarly => EvidenceError::Truncated { len: bytes.len() },
            BodyError::Malformed(detail) => EvidenceError::Decode(detail),
        })?;
        if evidence.schema_version != version {
            return Err(EvidenceError::MismatchedBodySchema {
                header: version,
                body: evidence.schema_version,
            });
        }
        Ok(evidence)
    }

    /// Decode an evidence FILE, also accepting the unframed format that builds
    /// v0.2.129 to v0.2.133 wrote.
    ///
    /// For files an operator chose, such as `fdev verify-merge --evidence`. Never
    /// pass it bytes from a peer: the unframed format is recognised by two bytes
    /// rather than the 8-byte magic, and a receive path that accepted it would
    /// accept a second wire format permanently. [`Self::decode`] is the decoder for
    /// untrusted input.
    ///
    /// A framed file is decoded exactly as [`Self::decode`] would decode it. An
    /// unframed one is raw bincode whose first field is `schema_version` (LE `u16`).
    /// Schema 2 is byte-compatible with the current struct and is decoded, through
    /// the same bounded [`decode_body`]. Schema 1 is not, because `settling` was
    /// added to `Violation` when the schema moved to 2.
    pub fn decode_file(bytes: &[u8]) -> Result<Self, EvidenceError> {
        if bytes.starts_with(EVIDENCE_MAGIC) {
            return Self::decode(bytes);
        }
        if bytes.len() < 2 {
            return Err(EvidenceError::BadMagic);
        }
        let legacy_version = u16::from_le_bytes([bytes[0], bytes[1]]);
        if legacy_version == 2 {
            return decode_body(bytes).map_err(|error| match error {
                // Hedged like the other two: an oversized file that happens to begin
                // `02 00` is not thereby evidence.
                BodyError::TooLarge { found } => EvidenceError::LegacyUndecodable(format!(
                    "it is {found} bytes, more than any evidence this build accepts \
                     ({MAX_EVIDENCE_ENCODED_BYTES})"
                )),
                BodyError::EndedEarly => {
                    EvidenceError::LegacyUndecodable("it ends early".to_string())
                }
                BodyError::Malformed(detail) => EvidenceError::LegacyUndecodable(detail),
            });
        }
        if legacy_version == 1 {
            return Err(EvidenceError::LegacyUnsupported {
                found: legacy_version,
            });
        }
        Err(EvidenceError::BadMagic)
    }
}

// `decode_file` decodes an unframed schema-2 payload with the CURRENT struct, which
// is right only while the current schema is 2. Bumping the schema has to revisit
// that branch, so the build refuses to compile until someone has.
//
// Editing the `2` below to match the new schema silences this without making the
// decision it exists to force: `decode_file` would then decode old unframed files
// with a struct they were never written with. The fix belongs in `decode_file`.
const _: () = assert!(
    EVIDENCE_SCHEMA_VERSION == 2,
    "EVIDENCE_SCHEMA_VERSION changed: decide in decode_file what happens to unframed \
     schema-2 files; changing the number in this assert is not that decision"
);

/// The bincode configuration evidence is decoded with.
///
/// Byte-compatible with `bincode::serialize`, which [`ConformanceEvidence::encode`]
/// uses: fixint and little-endian, like bincode 1.x's free functions. It differs
/// from `bincode::deserialize` in refusing bytes left over after a complete
/// payload, instead of ignoring them.
///
/// It deliberately sets no size limit, because bincode would ignore one:
/// deserializing from a slice replaces the configured limit with `Infinite`
/// (`internal::deserialize_seed` in bincode 1.3), so `.with_limit(..)` here would
/// read as a bound and bound nothing. The bound is the length check at the top of
/// [`decode_body`], which runs before bincode reads a byte.
///
/// What that leaves for allocation, stated precisely because a receive path will
/// rely on it: a `String` is bounded by the input, since the slice reader refuses a
/// declared length longer than the input that remains before allocating. A `Vec` is
/// not. serde preallocates `min(declared, 1 MiB / element size)` before reading a
/// single element, so a few dozen hostile bytes declaring `u64::MAX` elements for a
/// nested `Vec<Vec<u8>>` cost about 2 MiB of transient capacity before decoding
/// fails. Allocation is therefore bounded by the input plus a small constant, not by
/// the input alone.
fn evidence_bincode() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

/// Why a payload failed to decode, before the caller decides how to report it.
///
/// The two callers report the same failure differently: a framed file that ends
/// early is [`EvidenceError::Truncated`], while an unframed one that fails is
/// [`EvidenceError::LegacyUndecodable`], since it may not be evidence at all.
enum BodyError {
    /// Larger than [`MAX_EVIDENCE_ENCODED_BYTES`]; not parsed at all.
    TooLarge { found: usize },
    /// The bytes ran out before a complete payload.
    EndedEarly,
    /// Anything else, with bincode's description of it.
    Malformed(String),
}

/// Decode an evidence payload, refusing an oversized one before bincode reads it.
fn decode_body(body: &[u8]) -> Result<ConformanceEvidence, BodyError> {
    if body.len() > MAX_EVIDENCE_ENCODED_BYTES {
        return Err(BodyError::TooLarge { found: body.len() });
    }
    evidence_bincode().deserialize(body).map_err(|error| {
        if matches!(
            &*error,
            bincode::ErrorKind::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof
        ) {
            // Includes a length prefix claiming more bytes than the payload holds:
            // the payload ends before what it declares.
            BodyError::EndedEarly
        } else {
            BodyError::Malformed(error.to_string())
        }
    })
}

/// Refuse a sender-written text field longer than [`MAX_EVIDENCE_TEXT_BYTES`].
fn check_text_len(field: &'static str, text: &str) -> Result<(), EvidenceRejected> {
    if text.len() > MAX_EVIDENCE_TEXT_BYTES {
        return Err(EvidenceRejected::TextTooLong {
            field,
            found: text.len(),
            limit: MAX_EVIDENCE_TEXT_BYTES,
        });
    }
    Ok(())
}

/// Shorten `text` to at most `max` bytes without splitting a UTF-8 character.
fn truncate_text(text: &mut String, max: usize) {
    if text.len() <= max {
        return;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
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

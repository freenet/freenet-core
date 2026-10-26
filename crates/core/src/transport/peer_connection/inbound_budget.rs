//! Per-connection accounting of inbound stream reassembly state.
//!
//! [`InboundStreamBudget`] charges each admitted stream its declared length
//! plus its overhead ([`ChargeOverhead`]) against a stream count and a byte
//! allowance. The charge is held until ALL of that stream's state is gone:
//! for an operations stream, every clone of its fragment buffer (the
//! connection's handle map, the `StreamRegistry` row, the orphan registry
//! entry, and whatever consumer claimed it); for a legacy stream, the
//! `recv_stream` task, the `InboundStream` it owns, and its not yet consumed
//! output. Liveness is observed through a `Weak` to that state, so the charge
//! cannot be released early by a path that forgot to report a removal.
//!
//! Eviction of stale streams (idle, or past an age limit that traffic cannot
//! refresh) is decided here and carried out by the connection. Only new
//! bytes count as progress; a replayed fragment does not.
//!
//! When a stream completes it is copied into one buffer of its length while
//! its fragments are released, so for that moment it briefly takes up to its
//! charge plus its length; the charge itself lasts until the message is
//! handed on (legacy) or its consumers let go (operations).
//!
//! The bound is per peer: a UDP connection holds its budget through a
//! [`SharedInboundBudget`] handle, and a QUIC path to the same peer can be
//! built on that handle (`QuicPath::with_budget`), so both charge one
//! allowance. Charges are keyed by [`InboundKey`], which names the path, so
//! the same numeric stream id on both holds two separate charges. Node
//! reassembly memory is at most the number of peers times
//! `MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`. There is no node-wide
//! allowance; fairness between peers is left to the transport redesign.
//!
//! A gone UDP charge is released only by UDP code, at the points it was
//! released before the budget could be shared, so UDP's release timing is
//! exactly what it was (a UDP-only node releases as before). A QUIC path
//! reads a gone charge of its own as free, so the UDP side may release
//! those too (see [`Releaser`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};

use super::StreamId;
use super::streaming_buffer::LockFreeStreamBuffer;

/// Shared by a legacy stream's reassembly task, its `InboundStream`, and its
/// output until `recv()` takes it. Carries the payload bytes the stream
/// holds (its progress) and its state: active, then either complete or
/// cancelled, whichever is set first (the other then fails).
#[derive(Debug, Default)]
pub(in crate::transport) struct LegacyStreamToken {
    received_bytes: AtomicU64,
    state: AtomicU8,
}

const ACTIVE: u8 = 0;
const COMPLETE: u8 = 1;
const CANCELLED: u8 = 2;

impl LegacyStreamToken {
    pub(in crate::transport) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Report the stream's received payload bytes.
    pub(in crate::transport) fn report(&self, received: u64) {
        self.received_bytes.store(received, Ordering::Release);
    }

    fn transition(&self, to: u8) -> bool {
        self.state
            .compare_exchange(ACTIVE, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Record that the stream has every byte, contiguously. Returns false if
    /// it was cancelled first; it must then not be delivered.
    pub(in crate::transport) fn mark_complete(&self) -> bool {
        self.transition(COMPLETE)
    }

    /// Stop the stream: its task drops queued fragments and ends without a
    /// message. Returns false if it completed first; it is then delivered.
    pub(super) fn cancel(&self) -> bool {
        self.transition(CANCELLED)
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == CANCELLED
    }
}

/// What keeps a charged stream's reassembly state alive.
pub(in crate::transport) enum Liveness {
    /// An operations stream: alive while any clone of its buffer exists.
    Buffer(Weak<LockFreeStreamBuffer>),
    /// A legacy stream: alive while its token is held (see
    /// [`LegacyStreamToken`]).
    Task(Weak<LegacyStreamToken>),
}

impl Liveness {
    fn is_alive(&self) -> bool {
        match self {
            Liveness::Buffer(weak) => weak.strong_count() > 0,
            Liveness::Task(weak) => weak.strong_count() > 0,
        }
    }

    /// Whether the stream has all its bytes, contiguously (not merely as
    /// many bytes as declared), if its state is still alive.
    fn is_complete(&self) -> Option<bool> {
        match self {
            Liveness::Buffer(weak) => weak.upgrade().map(|b| b.is_complete()),
            Liveness::Task(weak) => weak
                .upgrade()
                .map(|t| t.state.load(Ordering::Acquire) == COMPLETE),
        }
    }

    /// Payload bytes the stream has received, if its state is still alive.
    fn received_bytes(&self) -> Option<u64> {
        match self {
            Liveness::Buffer(weak) => weak.upgrade().map(|b| b.received_bytes()),
            Liveness::Task(weak) => weak
                .upgrade()
                .map(|t| t.received_bytes.load(Ordering::Acquire)),
        }
    }
}

struct Charge {
    /// Bytes charged: the declared length plus the stream's overhead.
    bytes: u64,
    admitted_nanos: u64,
    /// Age after which the stream is evicted however much traffic it gets.
    max_age_nanos: u64,
    last_progress_nanos: u64,
    /// Most payload bytes the stream's state has reported holding.
    seen_bytes: u64,
    liveness: Liveness,
    /// Eviction has been requested; the charge stays until the state is gone.
    evicted: bool,
}

/// Why a new stream could not be charged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport) enum BudgetRejection {
    /// The connection already has the maximum number of charged streams.
    Streams { open: usize },
    /// The stream's length does not fit in the connection's byte allowance.
    Bytes { charged: u64 },
}

/// Why a stream was evicted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EvictionReason {
    /// No new data for longer than the idle timeout.
    Idle,
    /// Older than its age limit.
    Age,
}

/// Memory a stream costs beyond its payload bytes, charged with them.
#[derive(Debug, Clone, Copy)]
pub(in crate::transport) struct ChargeOverhead {
    /// Per stream: its buffer or reassembly task, handle, registry and map
    /// entries, channel, and first slot chunk.
    pub(super) per_stream: u64,
    /// Per fragment the stream may hold, beyond its payload bytes: see
    /// [`PER_FRAGMENT_OVERHEAD`]. Charged for every fragment number the
    /// declared length allows, so many tiny fragments are paid for.
    pub(super) per_fragment: u64,
}

/// Most an allocator rounds a fragment's payload allocation up by. Payloads
/// are at most `MAX_DATA_SIZE` (1130) bytes; jemalloc spaces its size classes
/// a quarter of a power of two apart, so the worst case is 1025 -> 1280.
pub(super) const PAYLOAD_SIZE_CLASS_SLACK: u64 = 256;

/// Memory a held fragment costs beyond its payload bytes, computed for
/// either path, plus the payload allocation's size-class slack:
/// - operations streams: an 8-byte slot pointer and a boxed `Bytes`;
/// - legacy streams: its share of the `BTreeMap<u32, Bytes>`. A node holds
///   up to 11 entries (`u32` key, `Bytes` value) and a 16-byte header, and a
///   non-root node is at least 5 entries full, so a leaf costs at most its
///   size / 5 per entry. Internal nodes also carry 12 child pointers and
///   have at least 6 children, so all of them together add at most an
///   internal node's size / 25 per entry.
///
/// The legacy share (about 104 B) is the larger.
pub(super) const PER_FRAGMENT_OVERHEAD: u64 = {
    let entry = (std::mem::size_of::<u32>() + std::mem::size_of::<bytes::Bytes>()) as u64;
    let leaf = 16 + 11 * entry;
    let internal = leaf + 12 * std::mem::size_of::<usize>() as u64;
    let btree = leaf.div_ceil(5) + internal.div_ceil(25);
    let slot = (std::mem::size_of::<std::sync::atomic::AtomicPtr<bytes::Bytes>>()
        + std::mem::size_of::<bytes::Bytes>()) as u64;
    let structure = if btree > slot { btree } else { slot };
    structure + PAYLOAD_SIZE_CLASS_SLACK
};

impl ChargeOverhead {
    /// A stream's fixed structures come to a few KiB (the first 256-slot chunk
    /// alone is 2 KiB, a legacy stream's 64-entry channel about 2.5 KiB, plus
    /// its task, handle and map entries); see [`PER_FRAGMENT_OVERHEAD`] for a
    /// held fragment.
    pub(in crate::transport) const DEFAULT: Self = Self {
        per_stream: 8 * 1024,
        per_fragment: PER_FRAGMENT_OVERHEAD,
    };

    /// No overhead, for tests that reason about payload bytes alone.
    #[cfg(test)]
    pub(super) const NONE: Self = Self {
        per_stream: 0,
        per_fragment: 0,
    };

    /// Bytes charged for a stream declaring `declared` payload bytes: the
    /// payload, the fixed overhead, and the per-fragment overhead for every
    /// fragment number it may use (one per full fragment, plus one).
    pub(in crate::transport) const fn cost(self, declared: u64) -> u64 {
        let fragments = declared.div_ceil(super::MAX_DATA_SIZE as u64) + 1;
        declared
            .saturating_add(self.per_stream)
            .saturating_add(fragments.saturating_mul(self.per_fragment))
    }
}

/// What a charge is held under. The variant names the path that charged
/// it, so charges from a UDP connection and a QUIC path sharing one budget
/// never meet, whatever their numeric ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::transport) enum InboundKey {
    /// A stream of the UDP connection, by its stream id.
    Udp(StreamId),
    /// A `Stream` frame of a QUIC path, by the id in its header.
    Quic(StreamId),
    /// A `Short` frame of a QUIC path, by a key the path picks.
    QuicShort(u32),
}

/// Who is releasing gone charges, which decides which ones it may release
/// ([`InboundStreamBudget::release_gone`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport) enum Releaser {
    /// The UDP connection: every gone charge. Its own, so its release
    /// timing stays what it was before the budget could be shared (the
    /// dedup against restarting a stream is `completed_inbound_streams`,
    /// not the held charge); a QUIC path's too, because the path treats a
    /// gone charge of its own as free (it releases one before testing an
    /// id), and without this a gone QUIC charge would hold room from the
    /// UDP side until the path next needed room, or for good once the path
    /// is gone.
    Udp,
    /// A QUIC path: only QUIC charges. A gone UDP charge stays until UDP
    /// code releases it: at its next `timeout_check` sweep (every 5 s) or
    /// its own admission under pressure. Until then the QUIC path can be
    /// refused for room that is free.
    Quic,
}

impl Releaser {
    fn may_release(self, key: &InboundKey) -> bool {
        match self {
            Self::Udp => true,
            Self::Quic => matches!(key, InboundKey::Quic(_) | InboundKey::QuicShort(_)),
        }
    }
}

/// One peer's [`InboundStreamBudget`], shared by its UDP connection and any
/// QUIC path built on it. The lock is taken only in synchronous sections,
/// never across an `.await`, and is not reentrant: do not call
/// [`Self::stream_count`] or [`Self::charged_bytes`] while holding
/// [`Self::lock`]. Lock order: the budget lock may be held while taking the
/// connection's `StreamRegistry` (DashMap shard) locks, never the reverse.
#[derive(Clone)]
pub(in crate::transport) struct SharedInboundBudget(Arc<Mutex<InboundStreamBudget>>);

impl SharedInboundBudget {
    pub(in crate::transport) fn new(max_streams: usize, max_bytes: u64) -> Self {
        Self(Arc::new(Mutex::new(InboundStreamBudget::new(
            max_streams,
            max_bytes,
        ))))
    }

    pub(in crate::transport) fn lock(&self) -> MutexGuard<'_, InboundStreamBudget> {
        self.0.lock()
    }

    pub(in crate::transport) fn stream_count(&self) -> usize {
        self.lock().stream_count()
    }

    #[cfg(test)]
    pub(in crate::transport) fn charged_bytes(&self) -> u64 {
        self.lock().charged_bytes()
    }
}

pub(in crate::transport) struct InboundStreamBudget {
    streams: HashMap<InboundKey, Charge>,
    charged_bytes: u64,
    max_streams: usize,
    max_bytes: u64,
    overhead: ChargeOverhead,
}

impl InboundStreamBudget {
    pub(in crate::transport) fn new(max_streams: usize, max_bytes: u64) -> Self {
        Self::with_overhead(max_streams, max_bytes, ChargeOverhead::DEFAULT)
    }

    pub(super) fn with_overhead(
        max_streams: usize,
        max_bytes: u64,
        overhead: ChargeOverhead,
    ) -> Self {
        Self {
            streams: HashMap::new(),
            charged_bytes: 0,
            max_streams,
            max_bytes,
            overhead,
        }
    }

    /// Whether `key` holds a charge (live, or evicted but not yet gone).
    pub(in crate::transport) fn contains(&self, key: InboundKey) -> bool {
        self.streams.contains_key(&key)
    }

    /// The UDP connection's operations streams whose buffers are still held
    /// somewhere.
    pub(super) fn live_buffer_streams(
        &self,
    ) -> impl Iterator<Item = (StreamId, Weak<LockFreeStreamBuffer>)> + '_ {
        self.streams
            .iter()
            .filter_map(|(key, charge)| match (key, &charge.liveness) {
                (InboundKey::Udp(id), Liveness::Buffer(weak)) if weak.strong_count() > 0 => {
                    Some((*id, weak.clone()))
                }
                _ => None,
            })
    }

    pub(in crate::transport) fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub(in crate::transport) fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }

    fn fits(&self, bytes: u64) -> Result<(), BudgetRejection> {
        if self.streams.len() >= self.max_streams {
            return Err(BudgetRejection::Streams {
                open: self.streams.len(),
            });
        }
        if self.charged_bytes.saturating_add(bytes) > self.max_bytes {
            return Err(BudgetRejection::Bytes {
                charged: self.charged_bytes,
            });
        }
        Ok(())
    }

    /// Whether a new stream declaring `declared` bytes fits this peer's
    /// limits (with its overhead; see [`ChargeOverhead`]). Releases the
    /// charges `by` may release whose state is already gone before refusing.
    pub(in crate::transport) fn check(
        &mut self,
        declared: u64,
        by: Releaser,
    ) -> Result<(), BudgetRejection> {
        let bytes = self.overhead.cost(declared);
        if self.fits(bytes).is_ok() {
            return Ok(());
        }
        self.release_gone(by);
        self.fits(bytes)
    }

    /// Charge a newly admitted stream declaring `declared` bytes (plus its
    /// overhead). Call only after `check` succeeded, under the same lock
    /// when the budget is shared (another path could take the room between).
    pub(in crate::transport) fn charge(
        &mut self,
        key: InboundKey,
        declared: u64,
        now_nanos: u64,
        max_age: Duration,
        liveness: Liveness,
    ) {
        debug_assert!(!self.streams.contains_key(&key));
        let bytes = self.overhead.cost(declared);
        let previous = self.streams.insert(
            key,
            Charge {
                bytes,
                admitted_nanos: now_nanos,
                max_age_nanos: u64::try_from(max_age.as_nanos()).unwrap_or(u64::MAX),
                last_progress_nanos: now_nanos,
                seen_bytes: liveness.received_bytes().unwrap_or(0),
                liveness,
                evicted: false,
            },
        );
        if let Some(previous) = previous {
            self.charged_bytes -= previous.bytes;
        }
        self.charged_bytes += bytes;
    }

    /// Record that `key` received new data.
    pub(in crate::transport) fn note_progress(&mut self, key: InboundKey, now_nanos: u64) {
        if let Some(charge) = self.streams.get_mut(&key) {
            charge.last_progress_nanos = now_nanos;
        }
    }

    /// Release `key`'s charge if its state is gone. Returns whether it was
    /// released.
    pub(in crate::transport) fn release_if_gone(&mut self, key: InboundKey) -> bool {
        let gone = self
            .streams
            .get(&key)
            .is_some_and(|charge| !charge.liveness.is_alive());
        if gone {
            if let Some(charge) = self.streams.remove(&key) {
                self.charged_bytes -= charge.bytes;
            }
        }
        gone
    }

    /// Release every charge `by` may release whose state is gone (see
    /// [`Releaser`]). Returns how many.
    pub(in crate::transport) fn release_gone(&mut self, by: Releaser) -> usize {
        let mut released = 0;
        let mut freed = 0;
        self.streams.retain(|key, charge| {
            let alive = !by.may_release(key) || charge.liveness.is_alive();
            if !alive {
                released += 1;
                freed += charge.bytes;
            }
            alive
        });
        self.charged_bytes -= freed;
        released
    }

    /// The UDP connection's streams to evict, with the reason: incomplete
    /// streams idle (no new data) for longer than `idle`, or admitted longer
    /// ago than their own age limit, whatever their traffic. Complete streams
    /// are never evicted (only their consumers still hold them). Charges of
    /// a QUIC path are left alone: the path evicts its own streams by a timer
    /// per stream.
    ///
    /// A legacy stream whose task reports holding more bytes than ever before
    /// has made progress as of `now_nanos` (its task, not the connection,
    /// knows whether a fragment was new; sweeps run every few seconds).
    /// Operations streams report new fragments through
    /// [`Self::note_progress`].
    ///
    /// Each is returned once; its charge remains until its state is gone.
    pub(super) fn take_expired(
        &mut self,
        now_nanos: u64,
        idle: Duration,
    ) -> Vec<(StreamId, EvictionReason)> {
        let idle_nanos = nanos(idle);
        let mut expired = Vec::new();
        for (key, charge) in self.streams.iter_mut() {
            let InboundKey::Udp(stream_id) = key else {
                continue;
            };
            if charge.evicted {
                continue;
            }
            let Some(received) = charge.liveness.received_bytes() else {
                continue; // state already gone; the charge is released separately
            };
            if matches!(charge.liveness, Liveness::Task(_)) && received > charge.seen_bytes {
                charge.seen_bytes = received;
                charge.last_progress_nanos = charge.last_progress_nanos.max(now_nanos);
            }
            if charge.liveness.is_complete() != Some(false) {
                continue;
            }
            let idle_for = now_nanos.saturating_sub(charge.last_progress_nanos);
            let age = now_nanos.saturating_sub(charge.admitted_nanos);
            let reason = if idle_for > idle_nanos {
                EvictionReason::Idle
            } else if age > charge.max_age_nanos {
                EvictionReason::Age
            } else {
                continue;
            };
            charge.evicted = true;
            expired.push((*stream_id, reason));
        }
        expired
    }
}

/// Most refused stream ids one connection remembers at once.
pub(super) const MAX_REMEMBERED_REFUSALS: usize = 4096;

/// Refused inbound stream ids, so a refused stream's later fragments are
/// dropped rather than admitted part-way through.
///
/// An id is never forgotten while its stream could still be arriving: it is
/// kept until the age limit a stream of its length would have had, after
/// which an admitted stream would have been evicted anyway. When
/// [`MAX_REMEMBERED_REFUSALS`] ids are held, a further refusal cannot be
/// remembered and the connection is closed instead.
#[derive(Default)]
pub(super) struct RefusedStreams {
    /// Id -> time (nanos) after which it is forgotten.
    ids: HashMap<StreamId, u64>,
    /// Earliest expiry among `ids` (0 when unknown or empty).
    next_expiry: u64,
}

impl RefusedStreams {
    /// Whether `stream_id` was refused and is still remembered.
    pub(super) fn contains(&mut self, stream_id: StreamId, now_nanos: u64) -> bool {
        match self.ids.get(&stream_id) {
            Some(&expiry) if expiry > now_nanos => true,
            Some(_) => {
                self.ids.remove(&stream_id);
                false
            }
            None => false,
        }
    }

    /// Whether no further refusal can be remembered (after dropping expired
    /// ids).
    fn is_full(&mut self, now_nanos: u64) -> bool {
        if self.ids.len() < MAX_REMEMBERED_REFUSALS {
            return false;
        }
        if now_nanos >= self.next_expiry {
            let mut next = u64::MAX;
            self.ids.retain(|_, expiry| {
                let keep = *expiry > now_nanos;
                if keep {
                    next = next.min(*expiry);
                }
                keep
            });
            self.next_expiry = next;
        }
        self.ids.len() >= MAX_REMEMBERED_REFUSALS
    }

    /// Remember `stream_id` as refused until `expiry_nanos`. Returns false,
    /// remembering nothing, if the memory is full.
    pub(super) fn remember(
        &mut self,
        stream_id: StreamId,
        expiry_nanos: u64,
        now_nanos: u64,
    ) -> bool {
        if self.is_full(now_nanos) {
            return false;
        }
        if self.ids.is_empty() || expiry_nanos < self.next_expiry {
            self.next_expiry = expiry_nanos;
        }
        self.ids.insert(stream_id, expiry_nanos);
        true
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Source-scrape helpers for the pins that one guard on a shared budget
/// spans an admission, on either path (AGENTS.md, "WHEN writing a
/// source-scrape pin test"). Each pin scrapes ANOTHER file, so its own
/// literals cannot satisfy it.
#[cfg(test)]
pub(in crate::transport) mod pin {
    /// The body of the function whose definition starts with `signature`,
    /// by brace matching from its first `{`, so methods work too, with
    /// comments stripped so a call inside a `/* .. */` block does not count.
    /// The signature must occur exactly once in `src`: a second occurrence
    /// could be matched instead and scope the scan to the wrong function.
    pub(in crate::transport) fn fn_body(src: &str, signature: &str) -> String {
        assert_eq!(
            src.matches(signature).count(),
            1,
            "`{signature}` must occur exactly once in the scraped source"
        );
        let start = src.find(signature).expect("counted above");
        let open = start
            + src[start..]
                .find('{')
                .unwrap_or_else(|| panic!("`{signature}` has no body"));
        let mut depth = 0usize;
        for (i, byte) in src.bytes().enumerate().skip(open) {
            match byte {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return crate::contract::source_pin_util::strip_comments(&src[open + 1..i]);
                    }
                }
                _ => {}
            }
        }
        panic!("unbalanced braces in `{signature}`");
    }

    /// Where `needle` occurs at statement position in `body` (preceded on
    /// its line by whitespace only, so a commented-out call does not
    /// count). It must occur so exactly once.
    pub(in crate::transport) fn statement_at(body: &str, needle: &str, what: &str) -> usize {
        let at: Vec<usize> = body
            .match_indices(needle)
            .map(|(i, _)| i)
            .filter(|i| {
                let line_start = body[..*i].rfind('\n').map_or(0, |n| n + 1);
                body[line_start..*i].trim().is_empty()
            })
            .collect();
        assert_eq!(
            at.len(),
            1,
            "{what}: `{needle}` must be called exactly once at statement position"
        );
        at[0]
    }

    /// In `body`: `lock`, then each of `steps`, then `charge`, each once at
    /// statement position and in that order, with nothing between the lock
    /// and the charge that locks again (any `lock(`, including
    /// `SharedInboundBudget::lock(&..)`) or gives the guard up for a while
    /// (`unlocked`). Returns the text after the charge call.
    pub(in crate::transport) fn assert_one_guard<'a>(
        body: &'a str,
        what: &str,
        lock: &str,
        steps: &[&str],
        charge: &str,
    ) -> &'a str {
        let locked = statement_at(body, lock, what) + lock.len();
        let mut at = locked;
        for step in steps.iter().chain([&charge]) {
            let next = statement_at(body, step, what);
            assert!(
                next > at,
                "{what}: `{step}` must come after the lock and the steps before it"
            );
            at = next;
        }
        let held = &body[locked..at];
        for forbidden in ["lock(", "unlocked"] {
            assert!(
                !held.contains(forbidden),
                "{what}: `{forbidden}` between taking the guard and the charge; the budget \
                 must stay locked from the check to the charge"
            );
        }
        &body[at + charge.len()..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const SECS: u64 = 1_000_000_000;
    const AGE: Duration = Duration::from_secs(3600);

    fn task() -> (Arc<LegacyStreamToken>, Liveness) {
        let token = LegacyStreamToken::new();
        let liveness = Liveness::Task(Arc::downgrade(&token));
        (token, liveness)
    }

    fn budget(max_streams: usize, max_bytes: u64) -> InboundStreamBudget {
        InboundStreamBudget::with_overhead(max_streams, max_bytes, ChargeOverhead::NONE)
    }

    /// The charge covers the declared payload, a fixed per-stream overhead,
    /// and an overhead for every fragment the declared length allows, so a
    /// stream sent as many tiny fragments is paid for.
    #[test]
    fn the_charge_includes_stream_and_fragment_overhead() {
        let f = super::super::MAX_DATA_SIZE as u64;
        let overhead = ChargeOverhead::DEFAULT;
        assert_eq!(
            overhead.cost(10 * f),
            10 * f + overhead.per_stream + 11 * overhead.per_fragment
        );
        let mut budget = InboundStreamBudget::new(4, u64::MAX);
        let (_token, liveness) = task();
        budget.charge(
            InboundKey::Udp(StreamId::next_transport_internal()),
            10 * f,
            0,
            AGE,
            liveness,
        );
        assert_eq!(budget.charged_bytes(), overhead.cost(10 * f));
    }

    #[test]
    fn charges_are_held_until_the_state_is_gone() {
        let mut budget = budget(4, 1_000);
        let id = InboundKey::Udp(StreamId::next_transport_internal());
        let (token, liveness) = task();
        budget.check(600, Releaser::Udp).unwrap();
        budget.charge(id, 600, 0, AGE, liveness);
        assert_eq!(budget.charged_bytes(), 600);

        assert!(!budget.release_if_gone(id), "state still alive");
        assert_eq!(budget.release_gone(Releaser::Udp), 0);
        assert_eq!(
            budget.check(500, Releaser::Udp),
            Err(BudgetRejection::Bytes { charged: 600 })
        );

        drop(token);
        // `check` releases gone charges before refusing.
        assert_eq!(budget.check(500, Releaser::Udp), Ok(()));
        assert_eq!(budget.charged_bytes(), 0);
        assert!(!budget.contains(id));
    }

    #[test]
    fn a_buffer_charge_lasts_while_any_clone_of_the_buffer_does() {
        let mut budget = budget(4, 1_000);
        let id = InboundKey::Udp(StreamId::next());
        let buffer = Arc::new(LockFreeStreamBuffer::new(100));
        let consumer_clone = Arc::clone(&buffer);
        budget.charge(id, 100, 0, AGE, Liveness::Buffer(Arc::downgrade(&buffer)));
        drop(buffer);
        assert!(!budget.release_if_gone(id), "a consumer still holds it");
        drop(consumer_clone);
        assert!(budget.release_if_gone(id));
        assert_eq!(budget.charged_bytes(), 0);
    }

    #[test]
    fn the_number_of_streams_is_bounded() {
        let mut budget = budget(2, u64::MAX);
        let mut tokens = Vec::new();
        for _ in 0..2 {
            let (token, liveness) = task();
            budget.check(1, Releaser::Udp).unwrap();
            budget.charge(
                InboundKey::Udp(StreamId::next_transport_internal()),
                1,
                0,
                AGE,
                liveness,
            );
            tokens.push(token);
        }
        assert_eq!(
            budget.check(1, Releaser::Udp),
            Err(BudgetRejection::Streams { open: 2 })
        );
        tokens.pop();
        assert_eq!(budget.check(1, Releaser::Udp), Ok(()));
        assert_eq!(budget.stream_count(), 1);
    }

    #[test]
    fn expiry_by_idleness_and_by_absolute_age() {
        let mut budget = budget(8, u64::MAX);
        let idle = Duration::from_secs(30);
        let max_age = Duration::from_secs(600);
        let (_t1, l1) = task();
        let (_t2, l2) = task();
        let quiet = StreamId::next_transport_internal();
        let busy = StreamId::next_transport_internal();
        budget.charge(InboundKey::Udp(quiet), 1, 0, max_age, l1);
        budget.charge(InboundKey::Udp(busy), 1, 0, max_age, l2);

        // Progress keeps `busy` from idling out, but not past the age limit.
        let mut now = 0;
        let mut expired = Vec::new();
        while now <= 601 * SECS {
            now += 10 * SECS;
            budget.note_progress(InboundKey::Udp(busy), now);
            expired.extend(
                budget
                    .take_expired(now, idle)
                    .into_iter()
                    .map(|(id, _)| (id, now)),
            );
        }
        assert_eq!(expired.len(), 2, "each stream is returned once");
        let (first, first_at) = expired[0];
        assert_eq!(first, quiet);
        assert!(first_at > 30 * SECS && first_at <= 40 * SECS);
        let (second, second_at) = expired[1];
        assert_eq!(second, busy);
        assert!(second_at > 600 * SECS && second_at <= 610 * SECS);
        // Evicted charges remain until their state is gone.
        assert_eq!(budget.stream_count(), 2);
    }

    const IDLE: Duration = Duration::from_secs(30);

    /// A complete stream is never evicted, however long its consumers hold it.
    #[test]
    fn complete_streams_are_not_evicted() {
        let mut budget = budget(8, u64::MAX);
        let done = StreamId::next_transport_internal();
        let (token, liveness) = task();
        budget.charge(
            InboundKey::Udp(done),
            10,
            0,
            Duration::from_secs(60),
            liveness,
        );
        token.report(10);
        token.mark_complete();
        assert_eq!(
            budget.take_expired(3600 * SECS, IDLE),
            vec![],
            "neither idle nor past its age limit counts for a complete stream"
        );
        assert_eq!(budget.stream_count(), 1, "charged until its state is gone");
    }

    /// The UDP connection's eviction sweep and its unclaimed-buffer cleanup
    /// see only its own charges on a budget shared with a QUIC path, which
    /// evicts its own streams: a QUIC charge with the same numeric id is
    /// neither returned nor marked evicted, and it still counts.
    #[test]
    fn udp_sweeps_see_only_udp_charges() {
        let mut budget = budget(8, u64::MAX);
        let id = StreamId::next();
        let buffer = Arc::new(LockFreeStreamBuffer::new(100));
        let quic_buffer = Arc::new(LockFreeStreamBuffer::new(100));
        let (_quic_token, quic_task) = task();
        budget.charge(
            InboundKey::Udp(id),
            100,
            0,
            AGE,
            Liveness::Buffer(Arc::downgrade(&buffer)),
        );
        budget.charge(
            InboundKey::Quic(id),
            100,
            0,
            AGE,
            Liveness::Buffer(Arc::downgrade(&quic_buffer)),
        );
        budget.charge(InboundKey::QuicShort(id.0), 100, 0, AGE, quic_task);

        let live: Vec<_> = budget.live_buffer_streams().map(|(id, _)| id).collect();
        assert_eq!(live, vec![id], "only the UDP buffer");
        assert_eq!(
            budget.take_expired(3600 * SECS, IDLE),
            vec![(id, EvictionReason::Idle)],
            "only the UDP stream"
        );
        assert_eq!(budget.take_expired(3600 * SECS, IDLE), vec![]);
        assert_eq!(budget.stream_count(), 3);
        assert_eq!(budget.charged_bytes(), 300);
    }

    /// A QUIC path's admission releases only gone QUIC charges: a gone UDP
    /// charge stays until UDP code releases it, at the points it always
    /// did. The UDP side releases gone charges of both, so a gone QUIC
    /// charge never holds UDP's room once the path stops needing room.
    #[test]
    fn a_quic_admission_never_releases_a_udp_charge() {
        let mut budget = budget(4, 200);
        let id = StreamId::next();
        let (udp_token, udp) = task();
        let (quic_token, quic) = task();
        let (short_token, short) = task();
        budget.charge(InboundKey::Udp(id), 100, 0, AGE, udp);
        budget.charge(InboundKey::Quic(id), 50, 0, AGE, quic);
        budget.charge(InboundKey::QuicShort(id.0), 50, 0, AGE, short);
        drop(udp_token);
        drop(quic_token);
        drop(short_token);

        assert_eq!(budget.check(1, Releaser::Quic), Ok(()));
        assert!(
            !budget.contains(InboundKey::Quic(id)),
            "its own gone charge"
        );
        assert!(
            !budget.contains(InboundKey::QuicShort(id.0)),
            "its own gone short-message charge"
        );
        assert!(budget.contains(InboundKey::Udp(id)), "not the UDP one");
        assert_eq!(
            budget.check(150, Releaser::Quic),
            Err(BudgetRejection::Bytes { charged: 100 })
        );
        assert_eq!(budget.release_gone(Releaser::Quic), 0);

        let (quic_token, quic) = task();
        budget.charge(InboundKey::QuicShort(1), 50, 0, AGE, quic);
        drop(quic_token);
        assert_eq!(budget.check(150, Releaser::Udp), Ok(()));
        assert_eq!(budget.stream_count(), 0, "the UDP side released both");
    }

    /// Admission counts the overhead: a stream whose payload fits the
    /// allowance but whose payload plus overhead does not is refused.
    #[test]
    fn admission_counts_the_overhead() {
        let declared = 100 * super::super::MAX_DATA_SIZE as u64;
        let cost = ChargeOverhead::DEFAULT.cost(declared);
        assert!(cost > declared, "premise");
        let mut budget = InboundStreamBudget::new(4, cost - 1);
        assert_eq!(
            budget.check(declared, Releaser::Udp),
            Err(BudgetRejection::Bytes { charged: 0 })
        );
        let mut budget = InboundStreamBudget::new(4, cost);
        assert_eq!(budget.check(declared, Releaser::Udp), Ok(()));
    }

    /// The per-fragment overhead covers the allocator's rounding of every
    /// payload size a fragment can have, under jemalloc's size classes (four
    /// per doubling).
    #[test]
    fn the_size_class_slack_covers_every_fragment_size() {
        use crate::util::alloc_count::jemalloc_class;
        let worst = (1..=super::super::MAX_DATA_SIZE as u64)
            .map(|n| jemalloc_class(n as usize) as u64 - n)
            .max()
            .unwrap();
        assert!(worst <= PAYLOAD_SIZE_CLASS_SLACK, "worst slack {worst}");
        assert!(
            PER_FRAGMENT_OVERHEAD
                >= PAYLOAD_SIZE_CLASS_SLACK
                    + (std::mem::size_of::<bytes::Bytes>()
                        + std::mem::size_of::<std::sync::atomic::AtomicPtr<bytes::Bytes>>())
                        as u64
        );
    }

    /// Refused ids are kept until their expiry, not dropped for newer ones;
    /// once the memory is full no further refusal is accepted until ids
    /// expire.
    #[test]
    fn refused_ids_are_remembered_until_they_expire() {
        // Expiry applies even while the memory has room (nothing is pruned).
        let mut refused = RefusedStreams::default();
        let id = StreamId::next_transport_internal();
        assert!(refused.remember(id, 10 * SECS, 0));
        assert!(refused.contains(id, 9 * SECS));
        assert!(!refused.contains(id, 10 * SECS), "expired");

        let mut refused = RefusedStreams::default();
        let first = StreamId::next_transport_internal();
        assert!(refused.remember(first, 100 * SECS, 0));
        for i in 1..MAX_REMEMBERED_REFUSALS {
            assert!(refused.remember(
                StreamId::next_transport_internal(),
                (200 + i as u64) * SECS,
                0
            ));
        }
        assert!(refused.is_full(0));
        assert!(refused.contains(first, 99 * SECS), "not displaced");
        // Once `first` has expired there is room again.
        assert!(!refused.is_full(100 * SECS));
        assert!(!refused.contains(first, 100 * SECS));
        assert!(refused.remember(StreamId::next_transport_internal(), 500 * SECS, 100 * SECS));
        assert!(refused.is_full(100 * SECS));
        assert!(
            !refused.remember(
                StreamId::next_transport_internal(),
                9_000 * SECS,
                100 * SECS
            ),
            "no room"
        );
    }
}

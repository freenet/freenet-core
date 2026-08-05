//! Originator target lists: telling a relayer who already got the broadcast.
//!
//! # The waste this exists to remove
//!
//! Contract broadcast is a mesh re-fan-out. `A` applies an update and sends it
//! to its advertised co-hosts `B`, `C`, `D`. `B` applies it and re-broadcasts
//! to ITS co-hosts — which include `C` and `D`, who already have it. Every node
//! does this simultaneously, so on the 0.2.119 fleet each node received ~18.6
//! byte-identical copies of every update: **74.6% of all received contract
//! bytes changed nothing** (issue #5147).
//!
//! `B` cannot currently tell that `C` already has it. The information that
//! would say so (`sender_summary_bytes`) rides ON the duplicate itself, so it
//! always arrives too late to prevent it.
//!
//! # The mechanism
//!
//! The originator attaches to its broadcast the list of peers it is sending
//! to. A relayer excludes listed peers from its own re-fan-out. That is all.
//!
//! Duplication requires `B` and `C` to be mutually-connected co-hosts — which
//! is the same condition that makes `A`'s target list overlap theirs. Waste and
//! fix live in the same regime: near the contract's key, where routing gravity
//! concentrates hosts and mutual connectivity is high, `targets(B)` is almost
//! entirely contained in `targets(A) ∪ {A}`. Measured suppression rises with
//! fan-out degree: 0.647 at degree 5, 0.863 at 14, 0.924 at 17. The fleet
//! median `broadcasted_to` is 16-17.
//!
//! # Why an exact list and not a Bloom filter
//!
//! The first design for #5147 was a Bloom covered-set. It was retired before
//! being shipped. At the median fan-out of 17 an exact list of 8-byte hashes is
//! ~144 bytes on the wire against the Bloom's 144, so it is not larger — and it
//! has **no false positives**. The Bloom's entire failure apparatus (a
//! per-broadcast salt, a saturation guard, union semantics across hops) existed
//! solely to manage false positives that an exact list does not have. A false
//! positive here means a peer that genuinely needed the update silently does
//! not get it until the ~5-minute interest heartbeat heals it, so removing that
//! failure mode outright is worth more than the bytes.
//!
//! 64-bit truncation collisions are ~2^-64 per pair. A collision would be
//! persistent for that peer pair rather than per-broadcast, which is why the
//! hash is transaction-seeded (below) — a colliding pair re-rolls every
//! broadcast instead of being stuck.
//!
//! # One hop only, and why that is also the security rule
//!
//! A relayer does NOT forward the list onward and does NOT union its own
//! targets into it. Measured hop-2 suppression is already 0.99, so multi-hop
//! buys nothing.
//!
//! It also closes an attack. The list is an attestation of the sender's own
//! actions: a malicious ORIGINATOR "suppressing" its own update is a non-attack
//! (it could simply not send). A malicious RELAYER attaching a fabricated
//! "everyone is covered" list to a genuine update could suppress third-party
//! re-fan-out for a cycle. The rule that closes it: **honor a list only from
//! the message that delivered the payload.** One-hop makes that free, and it is
//! enforced structurally — [`CoveredPeers`] is only ever read out of the
//! payload-bearing `BroadcastToV2` / `BroadcastToStreamingV2` variants, and the
//! re-fan-out builds a fresh list from its own targets rather than propagating
//! the one it received.
//!
//! # Transaction-seeded hashing
//!
//! Peer hashes are `blake3(tx_id ‖ pub_key)[..8]` rather than a bare key
//! prefix. This follows [`crate::operations::visited_peers`], whose rustdoc
//! documents the rationale: seeding on the transaction means the same peer
//! hashes differently in different broadcasts, so an observer cannot correlate
//! co-host sets across transactions to infer topology. The receiver
//! reconstructs the seed from the transaction id carried on the same message.
//!
//! Note that in production each fan-out leg mints its OWN transaction
//! (`broadcast_queue.rs`), so the list is built per-recipient. That is a
//! handful of 48-byte hashes per send and is not measurable next to the WASM
//! merge it precedes.
//!
//! # Provenance: how the list reaches the fan-out decision
//!
//! The list arrives on an inbound broadcast; the decision that consumes it
//! happens later, in `handle_broadcast_state_change`, after a round trip
//! through the contract handler and the WASM merge. Nothing on that path
//! carries the networking context — `NodeEvent::BroadcastStateChange` is
//! emitted by the executor on ANY committed state write, with no idea whether
//! the write came from a local client or an inbound broadcast.
//!
//! [`BroadcastCoverageStore`] bridges that gap without threading a networking
//! concept through the WASM/contract-handler layer: the apply registers its
//! coverage keyed by contract immediately before entering the contract handler,
//! and the fan-out takes it. See [`BroadcastCoverageStore`] for the ordering
//! and race argument, which is the load-bearing part.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use serde::{Deserialize, Serialize};

use crate::message::Transaction;
use crate::ring::PeerKey;
use crate::transport::TransportPublicKey;

/// A transaction-seeded, 8-byte truncated hash of a peer's public key.
pub(crate) type PeerHash = [u8; 8];

/// Maximum number of peers an originator will name in one broadcast.
///
/// At the fleet median fan-out of 16-17 this never binds; it bounds the tail
/// (`broadcasted_to` p90 is 58, max 138 on 0.2.119) so a single broadcast
/// cannot attach a half-kilobyte list to every leg of a large fan-out.
///
/// **Over-cap behaviour is truncation, never omission.** A truncated list names
/// fewer peers, so the relayer suppresses fewer of them and the result degrades
/// smoothly toward today's unsuppressed behaviour. Dropping the field entirely
/// would do the same thing, but a reader could not tell the two apart, and
/// "cap exceeded" would become indistinguishable from "peer does not implement
/// this". See `covered_peers_over_cap_truncates_rather_than_omitting`.
pub(crate) const MAX_COVERED_PEERS: usize = 64;

/// How long a registered coverage entry stays valid.
///
/// This is a backstop, not the normal lifetime. In the normal case an entry
/// lives from `update_contract`'s registration until the fan-out takes it —
/// one contract-handler round trip — and `CoverageRegistration` discards it
/// eagerly if the apply turns out not to change state (so no fan-out will ever
/// come for it). The TTL only catches entries orphaned by a dropped task.
///
/// Short on purpose: an orphaned entry is the one way a peer could be excluded
/// from a fan-out it actually needed, so it should not outlive its apply by
/// much.
const COVERAGE_TTL: Duration = Duration::from_secs(10);

/// The peers an originator says it already delivered this broadcast to.
///
/// Wire type. Rides on `UpdateMsg::BroadcastToV2` / `BroadcastToStreamingV2`,
/// which exist so that a pre-floor peer's bytes are untouched — bincode has no
/// field skipping, so this could not have been an added field or an `Option` on
/// the existing variants without changing what old peers decode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CoveredPeers {
    /// Sorted, deduplicated peer hashes. Sorted so the encoding is canonical
    /// for a given peer set and does not leak the originator's iteration order
    /// (which is location-correlated).
    hashes: Vec<PeerHash>,
}

impl CoveredPeers {
    /// An explicitly empty list: "I am telling you I covered nobody."
    ///
    /// Distinct from not sending a list at all. Used by the local-origin path,
    /// where there is no upstream sender and therefore nothing is covered.
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Build the list an originator attaches when sending under `tx`.
    ///
    /// `targets` is the originator's own fan-out set. Over
    /// [`MAX_COVERED_PEERS`] the list is truncated (after sorting, so the
    /// retained prefix is deterministic rather than iteration-order-dependent).
    pub(crate) fn from_targets<'a>(
        tx: &Transaction,
        targets: impl IntoIterator<Item = &'a TransportPublicKey>,
    ) -> Self {
        let mut hashes: Vec<PeerHash> = targets
            .into_iter()
            .map(|pub_key| peer_hash(tx, pub_key))
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        hashes.truncate(MAX_COVERED_PEERS);
        Self { hashes }
    }

    pub(crate) fn len(&self) -> usize {
        self.hashes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Resolve this list against the peers we might ourselves broadcast to.
    ///
    /// `candidates` should be the contract's advertised co-hosts — the exact
    /// set `get_broadcast_targets_update` draws from. Resolving at receive time
    /// rather than storing raw hashes is what lets
    /// [`BroadcastCoverageStore::register`] intersect two concurrent entries:
    /// each carries its own transaction seed, so raw hashes from different
    /// broadcasts live in different hash spaces and cannot be compared.
    ///
    /// A named peer we do not know is silently dropped — we would never have
    /// targeted it, so it is not a miss.
    pub(crate) fn resolve<'a>(
        &self,
        tx: &Transaction,
        candidates: impl IntoIterator<Item = &'a TransportPublicKey>,
    ) -> HashSet<PeerKey> {
        if self.hashes.is_empty() {
            return HashSet::new();
        }
        let named: HashSet<PeerHash> = self.hashes.iter().copied().collect();
        candidates
            .into_iter()
            .filter(|pub_key| named.contains(&peer_hash(tx, pub_key)))
            .map(|pub_key| PeerKey::from(pub_key.clone()))
            .collect()
    }
}

/// `blake3(tx_id ‖ pub_key)` truncated to 8 bytes.
///
/// Both sides must agree, so this must be a cryptographic digest with a stable
/// cross-version encoding — notably NOT `ahash`, whose output is not stable
/// across processes or releases.
fn peer_hash(tx: &Transaction, pub_key: &TransportPublicKey) -> PeerHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&tx.id_bytes());
    hasher.update(pub_key.as_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest.as_bytes()[..8]);
    out
}

/// Coverage awaiting the fan-out it belongs to.
///
/// # Ordering, which is what makes this sound
///
/// The write happens in `update_contract` BEFORE the `UpdateQuery` enters the
/// contract handler. The read happens in `handle_broadcast_state_change`, which
/// can only run AFTER the executor emitted `BroadcastStateChange`, which can
/// only happen after that same `UpdateQuery` committed. So an entry is always
/// present when its own fan-out reads it. There is no "did the write land in
/// time" race.
///
/// # The race there IS, and why it can only under-suppress
///
/// Two *different* updates to the *same* contract can be in flight at once, and
/// the entries are keyed by contract, so the fan-out for one could read the
/// entry registered by the other.
///
/// [`Self::register`] therefore **intersects** rather than replacing. Every
/// peer in the intersection was covered by *both* originators, so whichever
/// apply the fan-out actually belongs to, each suppressed peer genuinely had
/// the state. A mixed-up entry can only cause the relayer to suppress fewer
/// peers than it could have — which is by definition today's behaviour — and
/// never to suppress a peer that needed the update.
///
/// That direction is the whole point. Under-suppression costs bandwidth we are
/// already spending; over-suppression silently withholds an update until the
/// ~5-minute heartbeat.
///
/// Three things keep the intersection from being merely decorative rather than
/// the common case:
///
/// * The contract handler's fair queue serialises WASM merges per contract key,
///   so the window between registration and read is one queue slot.
/// * Duplicate copies of the same broadcast are dropped by
///   `broadcast_dedup_cache` BEFORE `update_contract`, so the ~18 duplicates of
///   one update register once between them, not 18 times.
/// * An apply that does not change state discards its entry immediately
///   ([`CoverageRegistration`]) rather than leaving it to expire. No-change
///   applies are the common case (97% of received contract bytes change
///   nothing), so without this the store would be full of entries that no
///   fan-out will ever come for, intersecting away every real one.
///
/// # Residual, stated rather than hidden
///
/// Commit paths that emit `BroadcastStateChange` WITHOUT going through
/// `update_contract` — the executor-internal replay, and the second emitter at
/// `executor_impl.rs`'s `broadcast_state_change` — do not register, so they can
/// take an entry belonging to a concurrent relay apply of the same contract.
/// That one IS over-suppression. It needs a same-contract PUT-or-replay
/// concurrent with a relayed update, inside one contract-handler round trip;
/// it is bounded by [`COVERAGE_TTL`] and healed by the interest heartbeat.
/// Closing it means threading provenance through the WASM/contract-handler
/// layer, which is the tradeoff #5147 chose against.
pub(crate) struct BroadcastCoverageStore {
    entries: DashMap<ContractInstanceId, CoverageEntry>,
}

/// Entry count above which [`BroadcastCoverageStore::register`] sweeps expired
/// entries before inserting a new contract.
///
/// `expires_at` governs an entry's VALIDITY on read, but nothing governed its
/// RESIDENCY: the only removals are `take`, `discard`, and same-key
/// replacement, so an entry that is registered, `keep()`-ed, and then never
/// consumed stays forever. Two paths produce exactly that, and both fire
/// hardest under load:
///
/// * `handle_broadcast_state_change` returns early for a BANNED contract and
///   for one with broken invariants, both BEFORE it takes the coverage — so
///   every applying broadcast on such a contract leaks an entry, precisely
///   during the storm conditions that set those flags (#4861 / #4903 shape);
/// * `try_notify_node_event(BroadcastStateChange)` is best-effort by design
///   (#4145), so a dropped event leaves a kept entry with no consumer.
///
/// Each leaked entry holds up to [`MAX_COVERED_PEERS`] public keys, so the
/// growth is monotonic in distinct contract ids touched. Sweeping only when
/// the map is already large keeps the hot path (a live contract re-registering
/// under its own key) free of the O(n) scan — that path hits the Occupied arm
/// and never reaches this check.
const SWEEP_THRESHOLD: usize = 512;

struct CoverageEntry {
    origin: BroadcastOrigin,
    expires_at: Instant,
}

/// Who already has the update this node is about to fan out.
///
/// Both halves are attestations by the peer that delivered the payload, and
/// both are honored only for the fan-out that follows that delivery:
///
/// * `sender` — the peer we received it FROM. Obviously has it; sending it
///   back is a guaranteed duplicate. `get_broadcast_targets_update` has always
///   meant to exclude this and never could, because the re-fan-out call site
///   had no sender to pass and handed in its own address instead (#5147).
/// * `covered` — the peers the sender says it also delivered to.
///
/// [`Self::local`] is the "this node produced the update itself" case: no
/// sender, nobody covered. It is a real value rather than an absent one, which
/// is what makes the store's intersection safe — see
/// [`BroadcastCoverageStore::register`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BroadcastOrigin {
    sender: Option<SocketAddr>,
    covered: HashSet<PeerKey>,
}

impl BroadcastOrigin {
    /// A client applied this update here: nothing is covered and there is no
    /// upstream sender to exclude.
    pub(crate) fn local() -> Self {
        Self::default()
    }

    /// The update arrived from `sender`, who says it also delivered to
    /// `covered`.
    pub(crate) fn relayed(sender: SocketAddr, covered: HashSet<PeerKey>) -> Self {
        Self {
            sender: Some(sender),
            covered,
        }
    }

    /// The peer that delivered this update, if it came from the network.
    pub(crate) fn sender(&self) -> Option<&SocketAddr> {
        self.sender.as_ref()
    }

    /// Whether the delivering peer already covered `pub_key`.
    pub(crate) fn covers(&self, pub_key: &TransportPublicKey) -> bool {
        !self.covered.is_empty() && self.covered.contains(&PeerKey::from(pub_key.clone()))
    }

    pub(crate) fn covered_len(&self) -> usize {
        self.covered.len()
    }

    /// Merge two claims that are live for the same contract at the same time.
    ///
    /// Both halves narrow rather than widen, so the result is a claim BOTH
    /// contributors stand behind:
    ///
    /// * `covered` intersects, so a suppressed peer was covered by every live
    ///   apply.
    /// * `sender` survives only if the two agree. Two different senders means
    ///   we cannot tell which delivery this fan-out belongs to, and excluding
    ///   the wrong one would withhold the update from a peer that needs it.
    fn narrow(&mut self, other: &BroadcastOrigin) {
        if self.sender != other.sender {
            self.sender = None;
        }
        self.covered.retain(|peer| other.covered.contains(peer));
    }
}

impl BroadcastCoverageStore {
    pub(crate) fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Record what the apply about to run already knows is covered.
    ///
    /// Intersects with any live entry for the same contract; see the type
    /// rustdoc for why intersection and not replacement.
    ///
    /// Registering an EMPTY set is meaningful and is what client-local applies
    /// do: it intersects everything to empty, so a concurrent local update
    /// disables suppression for the window rather than inheriting a relayed
    /// peer's list.
    pub(crate) fn register(&self, key: &ContractInstanceId, origin: BroadcastOrigin) {
        let now = Instant::now();
        let expires_at = now + COVERAGE_TTL;
        let mut inserted_new_contract = false;
        match self.entries.entry(*key) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let existing = occupied.get_mut();
                if existing.expires_at <= now {
                    // Stale: the apply that wrote it was orphaned. Replace
                    // rather than narrow — narrowing against a claim that no
                    // live apply stands behind would discard this apply's
                    // coverage for no reason.
                    //
                    // The deadline must be REPLACED too, not `min`-ed. The
                    // existing one is by definition in the past here, so
                    // keeping the earlier of the two would leave the fresh
                    // entry born already expired: `take` checks
                    // `expires_at > now`, fails, and hands back
                    // `BroadcastOrigin::local()`. That silently made this whole
                    // branch dead — safe in direction (under-suppression) but
                    // the exact opposite of what it says it does. Pinned by
                    // `replacing_a_stale_entry_yields_a_live_claim`.
                    existing.origin = origin;
                    existing.expires_at = expires_at;
                } else {
                    existing.origin.narrow(&origin);
                    // Keep the EARLIER deadline. The merged claim is only valid
                    // for as long as its shortest-lived contributor.
                    existing.expires_at = existing.expires_at.min(expires_at);
                }
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(CoverageEntry { origin, expires_at });
                inserted_new_contract = true;
            }
        }

        // Deliberately AFTER the match: `entry()` holds the shard guard for the
        // duration of that block, and `retain` wants every shard's guard. Doing
        // this inside the Vacant arm is the DashMap re-entrancy deadlock this
        // codebase has hit before.
        if inserted_new_contract {
            self.sweep_expired(now);
        }
    }

    /// Drop entries whose deadline has passed, but only once the map has grown
    /// past [`SWEEP_THRESHOLD`].
    ///
    /// See that constant for why residency needs its own mechanism at all.
    /// Bounded work: `retain` visits each shard once, and it runs only on the
    /// insertion of a contract not already tracked.
    fn sweep_expired(&self, now: Instant) {
        if self.entries.len() <= SWEEP_THRESHOLD {
            return;
        }
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    /// Take the coverage for a contract's fan-out, if any is live.
    ///
    /// Removes it: coverage belongs to one fan-out. A no-target retry
    /// re-emission that arrives later finds nothing and suppresses nothing,
    /// which is the safe direction.
    pub(crate) fn take(&self, key: &ContractInstanceId) -> BroadcastOrigin {
        match self.entries.remove(key) {
            Some((_, entry)) if entry.expires_at > Instant::now() => entry.origin,
            _ => BroadcastOrigin::local(),
        }
    }

    /// Drop a contract's coverage without consuming it as a fan-out would.
    ///
    /// Called when an apply completes without changing state (or fails), so no
    /// `BroadcastStateChange` will ever come for it.
    pub(crate) fn discard(&self, key: &ContractInstanceId) {
        self.entries.remove(key);
    }

    #[cfg(test)]
    pub(crate) fn live_entries(&self) -> usize {
        let now = Instant::now();
        self.entries
            .iter()
            .filter(|entry| entry.expires_at > now)
            .count()
    }

    /// Total entries RESIDENT, live or expired.
    ///
    /// Distinct from [`Self::live_entries`] on purpose: the gap between the two
    /// is what [`SWEEP_THRESHOLD`] exists to bound, and a test that only ever
    /// asked for the live count could not see an expired-entry leak at all.
    #[cfg(test)]
    pub(crate) fn resident_entries(&self) -> usize {
        self.entries.len()
    }

    /// Insert an entry with an explicit deadline.
    ///
    /// `Instant` is not injectable here (no `TimeSource` on this path), so
    /// without a seam there is no way to construct an EXPIRED entry, and the
    /// TTL — the whole orphan backstop — goes untested. It did: the stale-entry
    /// branch in `register` shipped inert because the deadline was `min`-ed
    /// against a past value, and no test could observe it.
    #[cfg(test)]
    pub(crate) fn insert_with_deadline(
        &self,
        key: &ContractInstanceId,
        origin: BroadcastOrigin,
        expires_at: Instant,
    ) {
        self.entries
            .insert(*key, CoverageEntry { origin, expires_at });
    }
}

impl Default for BroadcastCoverageStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Scope guard tying a registered coverage entry to the apply that wrote it.
///
/// Held across `update_contract`'s `UpdateQuery` round trip. If the apply
/// changes state, [`Self::keep`] hands the entry to the fan-out that is about
/// to run; otherwise the drop discards it, so an entry never outlives an apply
/// that will not produce a `BroadcastStateChange`.
///
/// Discarding the whole contract key rather than just this apply's contribution
/// is deliberate: a concurrent apply may have intersected into the same entry,
/// and dropping its work costs suppression (safe) whereas trying to subtract
/// only our own contribution would risk leaving coverage that no live apply
/// stands behind (unsafe).
pub(crate) struct CoverageRegistration<'a> {
    store: &'a BroadcastCoverageStore,
    key: ContractInstanceId,
    keep: bool,
}

impl<'a> CoverageRegistration<'a> {
    pub(crate) fn new(
        store: &'a BroadcastCoverageStore,
        key: ContractInstanceId,
        origin: BroadcastOrigin,
    ) -> Self {
        store.register(&key, origin);
        Self {
            store,
            key,
            keep: false,
        }
    }

    /// The apply changed state, so a fan-out is coming. Leave the entry for it.
    pub(crate) fn keep(mut self) {
        self.keep = true;
    }
}

impl Drop for CoverageRegistration<'_> {
    fn drop(&mut self) {
        if !self.keep {
            self.store.discard(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;

    fn pub_key() -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    fn tx() -> Transaction {
        Transaction::new::<crate::operations::update::UpdateMsg>()
    }

    /// A relayed claim from one fixed sender, so the tests that care about the
    /// `covered` half are not also varying the sender half.
    fn relayed(covered: HashSet<PeerKey>) -> BroadcastOrigin {
        BroadcastOrigin::relayed("127.0.0.1:1".parse().unwrap(), covered)
    }

    fn instance_id() -> ContractInstanceId {
        *crate::operations::test_utils::make_contract_key(1).id()
    }

    /// Replacing an EXPIRED entry must produce a claim the fan-out can use.
    ///
    /// The stale branch of `register` says it "replaces rather than narrows",
    /// but it originally then ran
    /// `existing.expires_at = existing.expires_at.min(expires_at)`. In that
    /// branch `existing.expires_at` is by definition already in the past, so
    /// `min` kept it: the fresh entry was born expired, `take` rejected it, and
    /// the branch was dead. Direction was safe (under-suppression), which is
    /// why nothing noticed — and no test existed that could, since `Instant`
    /// has no seam here.
    ///
    /// Mutation that must make this red: restore the `.min()` in the stale arm.
    #[test]
    fn replacing_a_stale_entry_yields_a_live_claim() {
        let store = BroadcastCoverageStore::new();
        let key = instance_id();
        let stale_peer = pub_key();
        let fresh_peer = pub_key();

        // An orphaned entry: registered by an apply whose fan-out never came.
        store.insert_with_deadline(
            &key,
            relayed(HashSet::from([PeerKey::from(stale_peer.clone())])),
            Instant::now() - Duration::from_secs(1),
        );

        store.register(
            &key,
            relayed(HashSet::from([PeerKey::from(fresh_peer.clone())])),
        );

        let taken = store.take(&key);
        assert!(
            taken.covers(&fresh_peer),
            "the replacing apply's coverage must survive — if this is empty the \
             entry was born expired and `take` handed back a local() claim, so \
             the stale-replacement branch is inert"
        );
        assert!(
            !taken.covers(&stale_peer),
            "the orphaned claim must be REPLACED, not merged: no live apply \
             stands behind it"
        );
    }

    /// An entry past its TTL must not suppress anything.
    ///
    /// The other half of the seam above: `take` is what enforces validity, and
    /// deleting its deadline check would make orphaned claims suppress peers
    /// indefinitely — the one way this design can withhold an update from a
    /// peer that needs it.
    #[test]
    fn an_expired_entry_suppresses_nothing() {
        let store = BroadcastCoverageStore::new();
        let key = instance_id();
        let peer = pub_key();

        store.insert_with_deadline(
            &key,
            relayed(HashSet::from([PeerKey::from(peer.clone())])),
            Instant::now() - Duration::from_millis(1),
        );

        let taken = store.take(&key);
        assert!(
            !taken.covers(&peer),
            "a claim past COVERAGE_TTL must be ignored; mutation: delete the \
             `expires_at > Instant::now()` guard in `take`"
        );
    }

    /// Orphaned entries must not accumulate without bound.
    ///
    /// `expires_at` governs validity on READ, but until the sweeper existed
    /// nothing governed RESIDENCY: `take`, `discard` and same-key replacement
    /// were the only removals, so a contract that registered coverage and then
    /// never fanned out (banned contract, broken invariants, dropped
    /// best-effort event) left an entry holding up to MAX_COVERED_PEERS keys
    /// forever — worst exactly under the storm conditions that cause it.
    ///
    /// Mutation that must make this red: make `sweep_expired` a no-op.
    #[test]
    fn expired_entries_are_reclaimed_once_the_map_grows() {
        let store = BroadcastCoverageStore::new();
        let past = Instant::now() - Duration::from_secs(1);

        // Orphan one entry per distinct contract, past the sweep threshold.
        for i in 0..(SWEEP_THRESHOLD + 2) {
            let mut bytes = [0u8; 32];
            bytes[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            store.insert_with_deadline(
                &ContractInstanceId::new(bytes),
                relayed(HashSet::from([PeerKey::from(pub_key())])),
                past,
            );
        }
        assert!(
            store.resident_entries() > SWEEP_THRESHOLD,
            "premise: the map must actually be over the sweep threshold"
        );
        assert_eq!(
            store.live_entries(),
            0,
            "premise: every seeded entry is expired"
        );

        // A new contract's registration is what triggers the sweep.
        let fresh_key = instance_id();
        store.register(
            &fresh_key,
            relayed(HashSet::from([PeerKey::from(pub_key())])),
        );

        assert_eq!(
            store.resident_entries(),
            1,
            "expired entries must be reclaimed, leaving only the live one; \
             found {} resident",
            store.resident_entries()
        );
        assert!(
            store.take(&fresh_key).covered_len() > 0,
            "the sweep must not evict the live entry it ran alongside"
        );
    }

    #[test]
    fn a_named_peer_resolves_and_an_unnamed_one_does_not() {
        let tx = tx();
        let named = pub_key();
        let unnamed = pub_key();
        let covered = CoveredPeers::from_targets(&tx, [&named]);

        let resolved = covered.resolve(&tx, [&named, &unnamed]);

        assert!(resolved.contains(&PeerKey::from(named)));
        assert!(!resolved.contains(&PeerKey::from(unnamed)));
        assert_eq!(resolved.len(), 1);
    }

    /// The hash is transaction-seeded, so the same peer set encodes differently
    /// per broadcast and cannot be correlated across transactions. Guards the
    /// privacy property in this module's rustdoc.
    #[test]
    fn the_same_peer_hashes_differently_under_a_different_transaction() {
        let peer = pub_key();
        let first = CoveredPeers::from_targets(&tx(), [&peer]);
        let second = CoveredPeers::from_targets(&tx(), [&peer]);
        assert_ne!(first, second);
    }

    /// A list resolved under the WRONG transaction must not match. This is the
    /// mechanical reason a relayer cannot replay someone else's list onto a
    /// different broadcast.
    #[test]
    fn a_list_does_not_resolve_under_a_different_transaction() {
        let peer = pub_key();
        let covered = CoveredPeers::from_targets(&tx(), [&peer]);
        assert!(covered.resolve(&tx(), [&peer]).is_empty());
    }

    #[test]
    fn covered_peers_over_cap_truncates_rather_than_omitting() {
        let tx = tx();
        let peers: Vec<TransportPublicKey> =
            (0..MAX_COVERED_PEERS * 2).map(|_| pub_key()).collect();
        let covered = CoveredPeers::from_targets(&tx, peers.iter());

        assert_eq!(
            covered.len(),
            MAX_COVERED_PEERS,
            "over-cap must truncate to the cap"
        );
        assert!(
            !covered.is_empty(),
            "over-cap must NEVER degrade to an empty/omitted list — that is \
             indistinguishable from a peer that does not implement this"
        );

        // Every retained entry must still be a genuine member: truncation may
        // only lose suppression, never invent it.
        let resolved = covered.resolve(&tx, peers.iter());
        assert_eq!(resolved.len(), MAX_COVERED_PEERS);
        assert!(resolved.iter().all(|peer| peers.contains(&peer.0)));
    }

    /// Encoding is canonical: the same peer set produces the same bytes
    /// regardless of the order the originator happened to iterate its targets.
    #[test]
    fn encoding_is_order_independent() {
        let tx = tx();
        let peers: Vec<TransportPublicKey> = (0..8).map(|_| pub_key()).collect();
        let forward = CoveredPeers::from_targets(&tx, peers.iter());
        let backward = CoveredPeers::from_targets(&tx, peers.iter().rev());
        assert_eq!(forward, backward);
    }

    #[test]
    fn a_registered_entry_is_taken_by_the_fan_out() {
        let store = BroadcastCoverageStore::new();
        let key = instance_id();
        let peer = PeerKey::from(pub_key());

        store.register(&key, relayed(HashSet::from([peer.clone()])));
        assert_eq!(store.take(&key), relayed(HashSet::from([peer])));
        assert_eq!(
            store.take(&key),
            BroadcastOrigin::local(),
            "coverage is consumed once"
        );
    }

    /// The load-bearing race property: concurrent registrations for one
    /// contract intersect, so a fan-out can only ever suppress peers that
    /// EVERY live apply agreed were covered.
    #[test]
    fn concurrent_registrations_intersect_and_never_over_suppress() {
        let store = BroadcastCoverageStore::new();
        let key = instance_id();
        let shared = PeerKey::from(pub_key());
        let only_first = PeerKey::from(pub_key());
        let only_second = PeerKey::from(pub_key());

        store.register(
            &key,
            relayed(HashSet::from([shared.clone(), only_first.clone()])),
        );
        store.register(
            &key,
            relayed(HashSet::from([shared.clone(), only_second.clone()])),
        );

        let taken = store.take(&key);
        assert_eq!(taken, relayed(HashSet::from([shared])));
        assert!(!taken.covers(&only_first.0));
        assert!(!taken.covers(&only_second.0));
    }

    /// A client-local apply registers an EMPTY set, which must collapse any
    /// concurrent relayed coverage to nothing — a local update must never be
    /// suppressed against a list that belongs to someone else's broadcast.
    #[test]
    fn an_empty_registration_collapses_concurrent_coverage() {
        let store = BroadcastCoverageStore::new();
        let key = instance_id();

        store.register(&key, relayed(HashSet::from([PeerKey::from(pub_key())])));
        store.register(&key, BroadcastOrigin::local());

        assert_eq!(store.take(&key).covered_len(), 0);
    }

    #[test]
    fn a_no_change_apply_discards_its_registration() {
        let store = BroadcastCoverageStore::new();
        let key = instance_id();
        let peer = PeerKey::from(pub_key());

        {
            let _registration =
                CoverageRegistration::new(&store, key, relayed(HashSet::from([peer])));
            assert_eq!(store.live_entries(), 1);
        }

        assert_eq!(
            store.live_entries(),
            0,
            "an apply that does not change state must not leave coverage \
             behind for an unrelated fan-out to consume"
        );
        assert_eq!(store.take(&key).covered_len(), 0);
    }

    #[test]
    fn a_changed_apply_keeps_its_registration_for_the_fan_out() {
        let store = BroadcastCoverageStore::new();
        let key = instance_id();
        let peer = PeerKey::from(pub_key());

        {
            let registration =
                CoverageRegistration::new(&store, key, relayed(HashSet::from([peer.clone()])));
            registration.keep();
        }

        assert_eq!(store.take(&key), relayed(HashSet::from([peer])));
    }

    #[test]
    fn coverage_for_one_contract_does_not_leak_into_another() {
        let store = BroadcastCoverageStore::new();
        let first = instance_id();
        let second = *crate::operations::test_utils::make_contract_key(2).id();
        assert_ne!(first, second);

        store.register(&first, relayed(HashSet::from([PeerKey::from(pub_key())])));
        assert_eq!(store.take(&second), BroadcastOrigin::local());
    }
}

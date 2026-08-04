//! Covered-peer set carried on a broadcast, so a relaying node can skip peers
//! the broadcast has already reached (#5147).
//!
//! # Why this exists
//!
//! Contract-state broadcast is a mesh re-fan-out, not tree forwarding. Every
//! co-host that receives an update applies it and then fans out to *its* own
//! advertised co-hosts, which in a well-connected co-host clique is very
//! nearly the same set. Measured fleet-wide on 0.2.119 (947 nodes, 12,798
//! node-hours, 2026-08-04): each node receives **18.6** byte-identical copies
//! of every state-changing delta, 97.0% of received contract bytes change
//! nothing, and the median node absorbs 505 MB/day of pure duplicate payload.
//! That duplication is the single largest bandwidth cost on the network.
//!
//! The duplication is genuinely `N` distinct senders, not one sender retrying:
//! the retry funnels all sit inside the zero-delivery branch of
//! `handle_broadcast_state_change` and cannot multiply a delivered payload,
//! same-sender repeats measure *below* chance (0.0261 observed vs 0.0379 for a
//! label-shuffled null), and per-node duplicate ratio tracks distinct-sender
//! fan-in at r = 0.63. So suppression can recover nearly all of it. See
//! freenet-core#5147 for the full measurement.
//!
//! # What this does
//!
//! A broadcast carries a compact set of the peers it has already been sent to.
//! Each relaying node
//!
//!   1. skips any of its own co-hosts already in the set, and
//!   2. unions its own targets into the set before forwarding.
//!
//! This is the payload counterpart of the summary-side exclusion #4965/#5003
//! established in [`super::proactive_summary_targets`] — "do not re-send to a
//! peer the broadcast already covered". The difference, and the reason this
//! needs the wire, is that there the covering send was made by *this* node, so
//! the information was local; here it was made by a different node.
//!
//! # Why a Bloom filter rather than an explicit list
//!
//! Fan-out degree is heavy-tailed: median 16, mean 24, p90 58, max 138
//! (0.2.119, 7,912 sampled dispatches). An explicit list of 32-byte public
//! keys would be up to ~4.4 KB on a message whose payload is often a delta of
//! a few hundred bytes. A fixed [`COVERED_SET_BYTES`]-byte filter is bounded,
//! unions in one `|=` pass across hops, and costs one hash per peer tested.
//!
//! A false positive skips a forward to a peer that was *not* actually covered.
//! That costs one delayed delivery, healed by the periodic InterestSync
//! anti-entropy round; it never corrupts state. A false negative is impossible,
//! so a peer that WAS covered is never re-sent to. The asymmetry is the right
//! way round: over-suppression is bounded and self-healing, and the saturation
//! guard below bounds it further.

/// Size of the filter in bytes. 128 bytes = 1024 bits.
///
/// Sized against the measured fan-out distribution with [`COVERED_SET_HASHES`]
/// = 6, where the false-positive rate is `(1 - e^(-k·n/m))^k`:
///
/// | peers in set `n` | false-positive rate |
/// |---|---|
/// | 17 (median-ish) | 0.003% |
/// | 32 | 0.05% |
/// | 64 | 0.09% |
/// | 138 (observed max) | 2.9% |
///
/// So even at the observed maximum single-hop fan-out the filter costs ~3% of
/// forwards, and at typical degree it is negligible. 128 bytes against a
/// duplicate-delivery factor of 18.6 is a trade worth making by three orders
/// of magnitude.
pub(crate) const COVERED_SET_BYTES: usize = 128;

/// Number of bit positions set per inserted peer.
pub(crate) const COVERED_SET_HASHES: u32 = 6;

/// Bytes this adds to every `BroadcastToWithCoverage` message on the wire:
/// [`COVERED_SET_BYTES`] of filter, plus bincode's 8-byte length prefix for the
/// byte sequence, plus the 8-byte per-broadcast salt.
///
/// Pinned by a test so the cost of the variant cannot drift unnoticed. For
/// scale: the median node currently absorbs 505 MB/day of duplicate payload
/// against ~1,081 MB/day of total received contract bytes, so even at one
/// broadcast per second this overhead is under 12 MB/day.
pub(crate) const COVERED_SET_WIRE_BYTES: usize = COVERED_SET_BYTES + 8 + 8;

/// Fraction of bits set above which the filter is treated as saturated and
/// [`CoveredSet::contains`] stops claiming coverage.
///
/// Without this, a filter that has accumulated peers across many hops
/// eventually returns `true` for nearly everything and silently halts
/// propagation — turning a bandwidth optimisation into a delivery outage whose
/// only backstop is the ~5-minute anti-entropy heartbeat. Past this fill the
/// filter has stopped carrying usable information, so the safe reading is "I
/// do not know", which means forward.
///
/// At 60% fill with k = 6 the false-positive rate is already `0.6^6` ≈ 4.7%,
/// and it climbs steeply after; that is the point where suppression stops
/// paying for itself.
const SATURATION_FILL: f32 = 0.6;

/// A bounded set of peers a broadcast has already been sent to.
///
/// The encoding is a constant [`COVERED_SET_WIRE_BYTES`] on the wire
/// regardless of how many peers are in it, so the per-broadcast overhead of
/// this variant is exactly predictable and cannot drift with fan-out degree.
#[serde_with::serde_as]
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CoveredSet {
    #[serde_as(as = "serde_with::Bytes")]
    bits: [u8; COVERED_SET_BYTES],
    /// Per-broadcast salt, chosen by the originator and inherited by every hop.
    ///
    /// **Load-bearing for correctness, not just hygiene.** Without it the bit
    /// positions for a peer depend only on its public key, so a false positive
    /// is deterministic AND correlated down the whole tree: every hop unions
    /// the originator's bits, so a peer that collides against those bits is
    /// suppressed by *every* relaying node. Since a contract's co-host set
    /// barely changes between updates, the same peer would then collide on
    /// update after update — turning a bounded "some forwards are redundant"
    /// into "this peer never receives this contract", with no self-healing.
    ///
    /// With a fresh salt per broadcast, collisions are independent across
    /// updates. A peer that is skipped once is picked up by the next
    /// broadcast, whose delta is computed against that peer's still-stale
    /// cached summary and therefore carries the missed update too.
    salt: u64,
}

impl std::fmt::Debug for CoveredSet {
    /// Deliberately summarises rather than dumping 128 bytes of hex into every
    /// message-level trace log — the fill fraction is the only property worth
    /// reading at a glance, and it is what the saturation guard keys on.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CoveredSet({}/{} bits set)",
            self.bits_set(),
            COVERED_SET_BYTES * 8
        )
    }
}

impl CoveredSet {
    /// Start a fresh covered set for one broadcast.
    ///
    /// `salt` must be unique per broadcast and identical across every hop of
    /// that broadcast — derive it from the originating transaction id or the
    /// payload hash, never from the contract key (which would be constant and
    /// reintroduce the persistent-collision failure this salt exists to
    /// prevent). See [`Self::salt`].
    pub(crate) fn new(salt: u64) -> Self {
        Self {
            bits: [0u8; COVERED_SET_BYTES],
            salt,
        }
    }

    pub(crate) fn salt(&self) -> u64 {
        self.salt
    }

    /// Derive the two 32-bit seeds used for double hashing.
    ///
    /// Deliberately ONE hash per peer, not [`COVERED_SET_HASHES`] of them:
    /// Kirsch-Mitzenmacher double hashing derives all k bit positions from a
    /// single 64-bit value as `h1 + i·h2` with no measurable loss of
    /// false-positive rate. `ahash` is the same non-cryptographic hash
    /// [`super::BroadcastDedupCache`] already uses on this path.
    ///
    /// A cryptographic hash here would be pure waste. Nothing about this
    /// filter is adversarial in a way a stronger hash would fix: the worst a
    /// peer can do by grinding its public key to collide is cause other peers
    /// to skip forwarding to it, i.e. deny itself timely updates, which it can
    /// do far more cheaply by simply not connecting.
    fn seeds(&self, pub_key: &crate::transport::TransportPublicKey) -> (u32, u32) {
        use ahash::AHasher;
        use std::hash::Hasher;

        let mut hasher = AHasher::default();
        hasher.write_u64(self.salt);
        hasher.write(pub_key.as_bytes());
        let h = hasher.finish();
        // h2 forced odd so it is coprime with the (power-of-two) bit count and
        // the k probes cannot degenerate into a short cycle.
        ((h >> 32) as u32, ((h as u32) | 1))
    }

    fn bit_positions(
        &self,
        pub_key: &crate::transport::TransportPublicKey,
    ) -> impl Iterator<Item = usize> {
        const BITS: u64 = (COVERED_SET_BYTES * 8) as u64;
        let (h1, h2) = self.seeds(pub_key);
        (0..COVERED_SET_HASHES).map(move |i| {
            (((h1 as u64).wrapping_add((i as u64).wrapping_mul(h2 as u64))) % BITS) as usize
        })
    }

    /// Record that the broadcast has been sent to `pub_key`.
    pub(crate) fn insert(&mut self, pub_key: &crate::transport::TransportPublicKey) {
        for pos in self.bit_positions(pub_key).collect::<Vec<_>>() {
            self.bits[pos / 8] |= 1 << (pos % 8);
        }
    }

    /// Whether the broadcast is believed to have already reached `pub_key`.
    ///
    /// Returns `false` once the filter is saturated — see [`SATURATION_FILL`].
    /// A `false` here always means "forward", so the failure direction is
    /// redundant traffic, never a lost update.
    pub(crate) fn contains(&self, pub_key: &crate::transport::TransportPublicKey) -> bool {
        if self.is_saturated() {
            return false;
        }
        self.bit_positions(pub_key)
            .all(|pos| self.bits[pos / 8] & (1 << (pos % 8)) != 0)
    }

    /// Merge another hop's covered set for the SAME broadcast into this one.
    ///
    /// Returns `false` and merges nothing if the salts differ. Two filters
    /// built under different salts index the same peer at different bit
    /// positions, so ORing them together would produce a filter whose set bits
    /// mean nothing under either salt — every membership test would then be a
    /// coin flip, and the false-positive rate this type is sized around would
    /// no longer bound anything. A mismatch means a caller has crossed two
    /// broadcasts; refusing is strictly safer than silently forwarding to
    /// everyone or silently suppressing everyone.
    #[must_use]
    pub(crate) fn union_with(&mut self, other: &CoveredSet) -> bool {
        if self.salt != other.salt {
            return false;
        }
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
        true
    }

    pub(crate) fn bits_set(&self) -> u32 {
        self.bits.iter().map(|b| b.count_ones()).sum()
    }

    pub(crate) fn is_saturated(&self) -> bool {
        self.bits_set() as f32 > SATURATION_FILL * (COVERED_SET_BYTES * 8) as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic distinct public keys, so a false-positive-rate failure
    /// reproduces exactly instead of being a coin flip. The bytes only need to
    /// be distinct and well-spread — `seeds` hashes them before use.
    fn key(n: usize) -> crate::transport::TransportPublicKey {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
        // Spread the index across the array so keys differing only in low bits
        // are not adjacent in any trivial way.
        bytes[8..16]
            .copy_from_slice(&((n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)).to_le_bytes());
        crate::transport::TransportPublicKey::from_bytes(bytes)
    }

    #[test]
    fn inserted_peers_are_always_reported_present() {
        // No false negatives: this is the property that makes suppression
        // safe to act on at all.
        let mut set = CoveredSet::new(0xD1CE_5A17);
        let keys: Vec<_> = (0..17).map(key).collect();
        for k in &keys {
            set.insert(k);
        }
        for k in &keys {
            assert!(set.contains(k), "inserted peer reported absent");
        }
    }

    #[test]
    fn absent_peers_are_rarely_reported_present_at_typical_fanout() {
        // Guards the sizing table in COVERED_SET_BYTES' rustdoc. At the
        // median fan-out of ~17 the false-positive rate should be far below
        // 1%; assert a loose 2% so the test is not itself flaky, while still
        // failing loudly if someone shrinks the filter or drops k.
        let mut set = CoveredSet::new(0xD1CE_5A17);
        for i in 0..17 {
            set.insert(&key(i));
        }
        let trials = 2000;
        let fp = (0..trials).filter(|i| set.contains(&key(1000 + i))).count();
        assert!(
            fp * 100 < trials * 2,
            "false-positive rate {fp}/{trials} exceeds 2% at n=17"
        );
    }

    #[test]
    fn union_preserves_membership_from_both_sides() {
        let (mut a, mut b) = (CoveredSet::new(7), CoveredSet::new(7));
        let (ka, kb) = (key(1), key(2));
        a.insert(&ka);
        b.insert(&kb);
        assert!(a.union_with(&b));
        assert!(a.contains(&ka) && a.contains(&kb));
    }

    #[test]
    fn union_refuses_a_different_broadcasts_filter() {
        // Filters under different salts index the same peer at different bit
        // positions, so ORing them yields bits that mean nothing under either
        // salt. Refusing is what keeps the false-positive rate a real bound.
        let (mut a, mut b) = (CoveredSet::new(1), CoveredSet::new(2));
        b.insert(&key(1));
        assert!(!a.union_with(&b), "must refuse a mismatched salt");
        assert_eq!(a.bits_set(), 0, "must not merge anything on refusal");
    }

    #[test]
    fn a_collision_does_not_persist_across_broadcasts() {
        // THE property the salt exists for. Unsalted, bit positions depend
        // only on the public key, so a peer that collides against a given
        // co-host set collides against it on EVERY update — and because each
        // hop inherits the originator's bits, it is suppressed tree-wide.
        // That is a permanently invisible peer, not a bounded waste.
        //
        // Find a key that false-positives under one salt, then assert it is
        // not condemned to false-positive under every other salt too.
        let members: Vec<_> = (0..17).map(key).collect();
        let build = |salt: u64| {
            let mut s = CoveredSet::new(salt);
            for m in &members {
                s.insert(m);
            }
            s
        };

        let first = build(1);
        let victim = (1000..60_000)
            .map(key)
            .find(|k| first.contains(k))
            .expect("expected at least one false positive in 59k probes");

        let escapes = (2..200u64).filter(|s| !build(*s).contains(&victim)).count();
        assert!(
            escapes > 150,
            "a peer that collides under one salt must be clear under nearly all \
             others; only {escapes}/198 salts cleared it — salting is not working"
        );
    }

    #[test]
    fn saturated_filter_reports_nothing_covered() {
        // The delivery-outage guard. A filter this full carries no usable
        // information, and the safe reading is "forward".
        let mut set = CoveredSet::new(0xD1CE_5A17);
        for i in 0..4000 {
            set.insert(&key(i));
        }
        assert!(set.is_saturated(), "test did not actually saturate");
        assert!(
            !set.contains(&key(0)),
            "a saturated filter must claim nothing is covered, or propagation halts"
        );
    }

    #[test]
    fn wire_encoding_is_constant_size_regardless_of_occupancy() {
        // The per-broadcast cost of this variant must not scale with fan-out
        // degree — that is the whole reason for a filter rather than an
        // explicit peer list. Assert the constant so a future encoding change
        // (e.g. switching to a Vec) trips CI instead of silently reintroducing
        // a degree-proportional cost.
        let empty = CoveredSet::new(0xD1CE_5A17);
        let mut full = CoveredSet::new(0xD1CE_5A17);
        for i in 0..138 {
            full.insert(&key(i));
        }
        for set in [&empty, &full] {
            let encoded = bincode::serialize(set).expect("serialize");
            assert_eq!(
                encoded.len(),
                COVERED_SET_WIRE_BYTES,
                "covered-set wire size drifted from the pinned constant"
            );
            let decoded: CoveredSet = bincode::deserialize(&encoded).expect("deserialize");
            assert_eq!(&decoded, set);
        }
    }
}

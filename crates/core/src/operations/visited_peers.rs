//! Bloom filter for tracking visited peers during GET/SUBSCRIBE operations.
//!
//! This module provides a space-efficient probabilistic data structure for tracking
//! which peers have been visited during contract search operations. Using a bloom filter
//! instead of a HashSet provides:
//!
//! - Fixed 64-byte size regardless of visited peer count
//! - Privacy protection via transaction-specific hashing
//! - Efficient serialization for network transmission

use std::net::SocketAddr;

use ahash::RandomState;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::config::MIN_COMPATIBLE_VERSION;
use crate::message::Transaction;
use crate::node::parse_crate_version;

/// Number of bits in the bloom filter (512 bits = 64 bytes).
const BLOOM_BITS: usize = 512;

/// Number of bytes in the bloom filter.
const BLOOM_BYTES: usize = BLOOM_BITS / 8;

/// Number of hash functions to use (double-hashing generates 4 from 2).
const NUM_HASHES: usize = 4;

/// First release whose peers mark the repaired indices of degenerate
/// double-hash steps (#5994, see [`VisitedPeers::repair_degenerate_step`]).
///
/// Per the wire-gated-floor rule in `docs/RELEASING.md`: the release that
/// first ships the repaired marking, then frozen. Checks switch to the repaired
/// indices only once [`REPAIRED_INDICES_SHIPPED_IN`] is set and the handshake
/// floor reaches this version ([`CHECK_REPAIRED_INDICES`]),
/// so a value below the shipping release makes peers test bits that release
/// never set.
pub(crate) const REPAIRED_INDICES_MIN_VERSION: (u8, u8, u16) = (0, 2, 151);

/// Has the repaired marking shipped, and in which release?
///
/// `Some(v)`: shipped in `v`, which must EQUAL the floor, frozen thereafter.
/// `None`: not yet shipped, so the floor is a prediction about the next release
/// and must stay strictly above the crate version. Checked by
/// `repaired_indices_floor_tracks_the_shipping_release`, mirroring
/// `ACK_VERSION_SHIPPED_IN`.
pub(crate) const REPAIRED_INDICES_SHIPPED_IN: Option<(u8, u8, u16)> = Some((0, 2, 151));

/// Whether [`VisitedPeers::probably_visited`] tests the repaired indices.
///
/// A filter carries bits from every peer on the path, some marking other
/// peers' addresses (a CONNECT joiner marks all its connections), and nothing
/// in it says which release set them. A repaired address that only an older
/// release marked has only one or two of its four repaired bits set, so
/// checking the repaired indices would read it as unvisited and route back to
/// it. Until the floor excludes those releases, this peer marks the repaired
/// indices (a superset of the old ones, so older peers still find them) but
/// checks the old ones, exactly as older releases do.
///
/// The floor admits only neighbours that mark the repaired indices. A peer
/// further back could still be older if a peer between runs a lower floor, so
/// raise the floor past [`REPAIRED_INDICES_MIN_VERSION`] once older releases
/// have left the network, not merely once it ships. Raising it is what switches
/// the check on, so `repaired_check_is_a_decision_not_a_side_effect` fails
/// until [`REPAIRED_CHECK_FLOOR_CONFIRMED`] records that decision.
const CHECK_REPAIRED_INDICES: bool = repaired_check_enabled(
    REPAIRED_INDICES_SHIPPED_IN,
    parse_crate_version(MIN_COMPATIBLE_VERSION),
);

/// Set to `true` by whoever raises `min-compatible-version` to at least
/// [`REPAIRED_INDICES_MIN_VERSION`], confirming that releases marking only the
/// unrepaired indices have left the network. Read only by a test: the floor
/// alone decides [`CHECK_REPAIRED_INDICES`], and the test stops the floor
/// reaching it as a side effect of a raise made for another reason.
#[cfg_attr(not(test), allow(dead_code))]
const REPAIRED_CHECK_FLOOR_CONFIRMED: bool = false;

const fn repaired_check_enabled(
    shipped: Option<(u8, u8, u16)>,
    handshake_floor: (u8, u8, u16),
) -> bool {
    shipped.is_some() && version_at_least(handshake_floor, REPAIRED_INDICES_MIN_VERSION)
}

const fn version_at_least(version: (u8, u8, u16), floor: (u8, u8, u16)) -> bool {
    if version.0 != floor.0 {
        return version.0 > floor.0;
    }
    if version.1 != floor.1 {
        return version.1 > floor.1;
    }
    version.2 >= floor.2
}

/// A bloom filter for tracking visited peers during contract search operations.
///
/// Uses the transaction ID as a hash seed to prevent topology inference attacks.
/// An observer cannot correlate visited sets across different transactions.
///
/// # False Positive Rates (k=4 hash functions, m=512 bits)
///
/// Measured per check over random transaction ids (#5994), checking the
/// unrepaired indices (today) and the repaired ones (once
/// [`CHECK_REPAIRED_INDICES`] is on):
///
/// | peers marked | unrepaired   | repaired     |
/// |--------------|--------------|--------------|
/// | 2            | 1 in ~22,000 | 1 in ~60,000 |
/// | 10           | 1 in ~3,700  | 1 in ~8,400  |
/// | 20           | 1 in ~1,100  | 1 in ~1,600  |
/// | 30           | 1 in ~380    | 1 in ~460    |
///
/// For small sets both are well above the textbook `(1 - e^(-kn/m))^k`. Apart
/// from the repaired steps, the four indices depend only on `h1` and `h2`
/// modulo 512, so two addresses collide on all four bits with probability about
/// `2 / 512^2` per pair. Up to about 10 peers that collision term dominates the
/// repaired column; from about 20 the textbook term does.
#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
pub struct VisitedPeers {
    /// The bloom filter bits (64 bytes = 512 bits).
    #[serde_as(as = "[_; BLOOM_BYTES]")]
    bits: [u8; BLOOM_BYTES],
    /// Hash keys derived from transaction ID for privacy.
    /// Skipped during serialization - receiver reconstructs from transaction ID.
    #[serde(skip)]
    hash_keys: (u64, u64),
}

impl std::fmt::Debug for VisitedPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set_bits = self.bits.iter().map(|b| b.count_ones()).sum::<u32>();
        f.debug_struct("VisitedPeers")
            .field("set_bits", &set_bits)
            .field("total_bits", &BLOOM_BITS)
            .finish()
    }
}

impl VisitedPeers {
    /// Creates a new empty bloom filter using the transaction ID as hash key.
    ///
    /// The transaction ID is used to derive hash keys, ensuring that:
    /// - The same peer address produces different hashes in different transactions
    /// - An observer cannot correlate visited sets across transactions
    pub fn new(tx: &Transaction) -> Self {
        let tx_bytes = tx.id_bytes();
        Self {
            bits: [0u8; BLOOM_BYTES],
            hash_keys: Self::derive_hash_keys(&tx_bytes),
        }
    }

    /// Sets the hash keys from a transaction after deserialization.
    ///
    /// When a VisitedPeers is deserialized, hash_keys are zeroed. This method
    /// must be called to restore them from the transaction ID before using
    /// `mark_visited` or `probably_visited`.
    pub fn with_transaction(mut self, tx: &Transaction) -> Self {
        let tx_bytes = tx.id_bytes();
        self.hash_keys = Self::derive_hash_keys(&tx_bytes);
        self
    }

    /// Derives hash keys from transaction bytes.
    fn derive_hash_keys(tx_bytes: &[u8; 16]) -> (u64, u64) {
        let key0 = u64::from_le_bytes([
            tx_bytes[0],
            tx_bytes[1],
            tx_bytes[2],
            tx_bytes[3],
            tx_bytes[4],
            tx_bytes[5],
            tx_bytes[6],
            tx_bytes[7],
        ]);
        let key1 = u64::from_le_bytes([
            tx_bytes[8],
            tx_bytes[9],
            tx_bytes[10],
            tx_bytes[11],
            tx_bytes[12],
            tx_bytes[13],
            tx_bytes[14],
            tx_bytes[15],
        ]);
        (key0, key1)
    }

    /// Marks a peer address as visited.
    ///
    /// Always sets the repaired indices, a superset of the unrepaired ones, so
    /// peers checking either set find the address.
    pub fn mark_visited(&mut self, addr: SocketAddr) {
        for idx in self.hash_indices(&addr, true) {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }

    /// Checks if a peer address has probably been visited.
    ///
    /// Returns `true` if the address is probably in the set (may have false positives),
    /// or `false` if the address is definitely not in the set (no false negatives).
    pub fn probably_visited(&self, addr: SocketAddr) -> bool {
        self.probably_visited_with(addr, CHECK_REPAIRED_INDICES)
    }

    fn probably_visited_with(&self, addr: SocketAddr, repaired: bool) -> bool {
        for idx in self.hash_indices(&addr, repaired) {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            if self.bits[byte_idx] & (1 << bit_idx) == 0 {
                return false;
            }
        }
        true
    }

    /// Generates bloom filter indices using double-hashing.
    ///
    /// Uses the technique from "Less Hashing, Same Performance: Building a Better
    /// Bloom Filter" (Kirsch & Mitzenmacher, 2006). Double-hashing computes h1 and h2,
    /// then generates k hashes as h1 + i*h2 for i in 0..k.
    /// This is more efficient than computing k independent hashes and provides
    /// equivalent probabilistic guarantees.
    ///
    /// `repaired` applies [`Self::repair_degenerate_step`] to the steps where
    /// double hashing repeats an index.
    fn hash_indices(&self, addr: &SocketAddr, repaired: bool) -> [usize; NUM_HASHES] {
        // Use two independent hash functions with different seeds.
        // The constants are derived from the golden ratio (phi) and are commonly
        // used in hash function mixing to ensure good bit distribution.
        // 0x9e3779b97f4a7c15 = 2^64 / phi (golden ratio constant)
        // 0x517cc1b727220a95 = another well-distributed odd constant
        let state1 = RandomState::with_seeds(self.hash_keys.0, self.hash_keys.1, 0, 0);

        let state2 = RandomState::with_seeds(
            self.hash_keys.0.wrapping_add(0x9e3779b97f4a7c15),
            self.hash_keys.1.wrapping_add(0x517cc1b727220a95),
            0,
            0,
        );

        // Compute two independent base hashes
        let h1 = state1.hash_one(addr);
        let h2 = state2.hash_one(addr);

        // Generate NUM_HASHES indices using double-hashing: h1 + i*h2
        let indices = [
            (h1 as usize) % BLOOM_BITS,
            (h1.wrapping_add(h2) as usize) % BLOOM_BITS,
            (h1.wrapping_add(h2.wrapping_mul(2)) as usize) % BLOOM_BITS,
            (h1.wrapping_add(h2.wrapping_mul(3)) as usize) % BLOOM_BITS,
        ];
        if repaired {
            Self::repair_degenerate_step(indices, h2)
        } else {
            indices
        }
    }

    /// Replaces the repeated indices double hashing yields when `h2` is a
    /// multiple of `BLOOM_BITS / 2`, so every address sets 4 distinct bits.
    ///
    /// With `BLOOM_BITS` a power of two, `h1 + i*h2` repeats an index only when
    /// `h2 % BLOOM_BITS` is 0 (one distinct bit) or `BLOOM_BITS / 2` (two). For
    /// 1 address in 256 per transaction the filter therefore tested 1 or 2 bits
    /// instead of 4: about two thirds of the false-positive excess measured in
    /// #5994 with 2 peers marked.
    ///
    /// The receiver recomputes indices from the transaction id, so peers on
    /// different releases must agree on them. Every other address keeps its
    /// indices exactly, and a repaired address keeps its old bit or bits
    /// (`base`, plus `base + BLOOM_BITS / 2` when the step was `BLOOM_BITS / 2`)
    /// among its four, so a peer checking the old indices still finds every address
    /// marked with the repaired ones. The reverse does not hold, which is why
    /// checking them waits for [`CHECK_REPAIRED_INDICES`].
    fn repair_degenerate_step(indices: [usize; NUM_HASHES], h2: u64) -> [usize; NUM_HASHES] {
        const HALF: usize = BLOOM_BITS / 2;
        if (h2 as usize) % HALF != 0 {
            return indices;
        }
        // `odd` is never 0 or HALF, so the four indices are distinct. It comes
        // from h2's bits above the ones that made the step degenerate.
        let odd = ((h2 >> BLOOM_BITS.trailing_zeros()) as usize % BLOOM_BITS) | 1;
        let base = indices[0];
        [
            base,
            (base + HALF) % BLOOM_BITS,
            (base + odd) % BLOOM_BITS,
            (base + HALF + odd) % BLOOM_BITS,
        ]
    }
}

/// Default implementation creates an empty bloom filter with zero hash keys.
/// The bloom filter won't work correctly until hash keys are set via `with_transaction()`.
/// This is useful for tests or when the bloom filter will be restored after deserialization.
impl Default for VisitedPeers {
    fn default() -> Self {
        Self {
            bits: [0u8; BLOOM_BYTES],
            hash_keys: (0, 0),
        }
    }
}

/// Implement Contains trait for use with k_closest_potentially_hosting.
impl crate::util::Contains<SocketAddr> for VisitedPeers {
    fn has_element(&self, target: SocketAddr) -> bool {
        self.probably_visited(target)
    }
}

/// Also implement for references.
impl crate::util::Contains<SocketAddr> for &VisitedPeers {
    fn has_element(&self, target: SocketAddr) -> bool {
        self.probably_visited(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Transaction;
    use crate::operations::get::GetMsg;

    fn test_transaction() -> Transaction {
        Transaction::new::<GetMsg>()
    }

    const GOLDEN_KEY1: u64 = 0xfedc_ba98_7654_3210;

    fn with_keys(key0: u64, key1: u64) -> VisitedPeers {
        VisitedPeers {
            hash_keys: (key0, key1),
            ..VisitedPeers::default()
        }
    }

    fn distinct(indices: &[usize]) -> usize {
        indices
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    /// Peers on different releases must derive identical indices: the bits
    /// travel on the wire and the receiver recomputes them from the transaction
    /// id. These values are what releases before #5994 compute, so a change to
    /// the index derivation, an ahash upgrade or a std `Hash` impl change for
    /// `SocketAddr` fails here instead of silently splitting the network's view
    /// of visited peers. It only sees the hasher this test build compiles:
    /// ahash switches to an AES hasher on x86 builds with the `aes` target
    /// feature (e.g. `-C target-cpu=native`), which no release target enables.
    #[test]
    fn hash_indices_match_previous_releases() {
        let visited = with_keys(0x0123_4567_89ab_cdef, GOLDEN_KEY1);
        for (addr, expected) in [
            ("10.0.0.1:31337", [448, 77, 218, 359]),
            ("192.168.1.1:9000", [411, 448, 485, 10]),
            ("[2001:db8::1]:8080", [268, 128, 500, 360]),
            ("127.0.0.1:8000", [299, 205, 111, 17]),
        ] {
            let addr: SocketAddr = addr.parse().unwrap();
            for repaired in [false, true] {
                assert_eq!(visited.hash_indices(&addr, repaired), expected, "{addr}");
            }
        }
    }

    /// The receiving half of the same contract: the hash keys a receiver
    /// derives from the transaction id bytes.
    #[test]
    fn hash_keys_match_previous_releases() {
        let id: [u8; 16] = std::array::from_fn(|i| (i as u8).wrapping_mul(17).wrapping_add(3));
        assert_eq!(
            VisitedPeers::derive_hash_keys(&id),
            (0x7a69_5847_3625_1403, 0x02f1_e0cf_bead_9c8b)
        );
    }

    /// Key0 values for which "10.0.0.3:31337" hits each degenerate step, with
    /// the indices releases before #5994 derived for it (`h2 % 512 == 0`, then
    /// `h2 % 512 == 256`) and the repaired ones. Found by trying key0 = 0, 1,
    /// 2, ... against the unrepaired derivation; like the golden vectors they
    /// assume this build's hasher.
    const DEGENERATE_ADDR: &str = "10.0.0.3:31337";
    const DEGENERATE_CASES: [(u64, [usize; NUM_HASHES]); 2] =
        [(1039, [147, 147, 147, 147]), (1193, [296, 40, 296, 40])];
    const DEGENERATE_REPAIRED: [[usize; NUM_HASHES]; 2] =
        [[147, 403, 432, 176], [296, 40, 143, 399]];

    /// Once checks use the repaired indices, every release must derive the
    /// same ones, so they are pinned like the unrepaired golden vectors.
    #[test]
    fn repaired_indices_are_pinned() {
        let addr: SocketAddr = DEGENERATE_ADDR.parse().unwrap();
        for ((key0, _), expected) in DEGENERATE_CASES.into_iter().zip(DEGENERATE_REPAIRED) {
            let visited = with_keys(key0, GOLDEN_KEY1);
            assert_eq!(visited.hash_indices(&addr, true), expected, "key0={key0}");
        }
    }

    /// Whatever the address and keys, a marked address passes the check of
    /// this release and of every older one: neither may produce a false
    /// negative.
    #[test]
    fn every_marked_address_passes_both_checks() {
        for i in 0..20_000u64 {
            let mut visited = with_keys(
                i.wrapping_mul(0x9e37_79b9_7f4a_7c15),
                i.wrapping_mul(0xbf58_476d_1ce4_e5b9),
            );
            let addr = SocketAddr::from(([10, (i >> 16) as u8, (i >> 8) as u8, i as u8], 4000));
            visited.mark_visited(addr);
            assert!(visited.probably_visited_with(addr, false), "{addr} i={i}");
            assert!(visited.probably_visited_with(addr, true), "{addr} i={i}");
        }
    }

    #[test]
    fn degenerate_step_sets_four_distinct_bits_including_the_old_ones() {
        let addr: SocketAddr = DEGENERATE_ADDR.parse().unwrap();
        for (key0, old) in DEGENERATE_CASES {
            let visited = with_keys(key0, GOLDEN_KEY1);
            assert_eq!(visited.hash_indices(&addr, false), old, "key0={key0}");
            let indices = visited.hash_indices(&addr, true);
            assert_eq!(distinct(&indices), NUM_HASHES, "key0={key0}: {indices:?}");
            for bit in old {
                assert!(
                    indices.contains(&bit),
                    "key0={key0}: {indices:?} dropped {bit}, which older peers test"
                );
            }
        }
    }

    /// #5994: an address whose double-hash step was degenerate read as visited
    /// as soon as its one or two bits were set by other addresses. The repaired
    /// check does not; the unrepaired one must, because those are exactly the
    /// bits an older release sets when it marks the address.
    #[test]
    fn only_the_repaired_check_needs_more_than_the_old_bits() {
        let addr: SocketAddr = DEGENERATE_ADDR.parse().unwrap();
        for (key0, old) in DEGENERATE_CASES {
            let mut visited = with_keys(key0, GOLDEN_KEY1);
            for bit in old {
                visited.bits[bit / 8] |= 1 << (bit % 8);
            }
            assert!(
                !visited.probably_visited_with(addr, true),
                "key0={key0}: bits {old:?} alone passed the repaired check"
            );
            assert!(
                visited.probably_visited_with(addr, false),
                "key0={key0}: an address older releases marked read as unvisited"
            );
            assert_eq!(
                visited.probably_visited(addr),
                !CHECK_REPAIRED_INDICES,
                "key0={key0}: probably_visited ignores CHECK_REPAIRED_INDICES"
            );
        }
    }

    /// Marking sets the repaired indices, so both checks find the address:
    /// this release's and every older release's.
    #[test]
    fn marked_degenerate_address_passes_both_checks() {
        let addr: SocketAddr = DEGENERATE_ADDR.parse().unwrap();
        for (key0, _) in DEGENERATE_CASES {
            let mut visited = with_keys(key0, GOLDEN_KEY1);
            visited.mark_visited(addr);
            assert!(visited.probably_visited_with(addr, false), "key0={key0}");
            assert!(visited.probably_visited_with(addr, true), "key0={key0}");
        }
    }

    /// The repaired check must stay off until the handshake floor excludes
    /// every release that marks only the unrepaired indices; see
    /// [`CHECK_REPAIRED_INDICES`].
    #[test]
    fn repaired_check_waits_for_the_floor() {
        assert!(version_at_least((0, 2, 151), (0, 2, 151)));
        assert!(version_at_least((0, 2, 152), (0, 2, 151)));
        assert!(version_at_least((0, 3, 0), (0, 2, 151)));
        assert!(version_at_least((1, 0, 0), (0, 2, 151)));
        assert!(!version_at_least((0, 2, 150), (0, 2, 151)));
        assert!(!version_at_least((0, 1, 999), (0, 2, 151)));
        assert!(!version_at_least((0, 2, 151), (1, 0, 0)));

        let shipped = Some(REPAIRED_INDICES_MIN_VERSION);
        assert!(repaired_check_enabled(
            shipped,
            REPAIRED_INDICES_MIN_VERSION
        ));
        assert!(repaired_check_enabled(shipped, (u8::MAX, 0, 0)));
        assert!(!repaired_check_enabled(shipped, (0, 0, 0)));
        assert!(
            !repaired_check_enabled(None, (u8::MAX, 0, 0)),
            "the check switched on before the repaired marking shipped"
        );
        assert_eq!(
            CHECK_REPAIRED_INDICES,
            repaired_check_enabled(
                REPAIRED_INDICES_SHIPPED_IN,
                parse_crate_version(MIN_COMPATIBLE_VERSION)
            )
        );
    }

    /// Raising `min-compatible-version` to [`REPAIRED_INDICES_MIN_VERSION`] or
    /// beyond switches the repaired check on. That must be a decision about
    /// older releases having left the network, not a side effect of a raise
    /// made for another reason (or of `scripts/release.sh` defaulting the floor
    /// to the previous release, #5833).
    #[test]
    fn repaired_check_is_a_decision_not_a_side_effect() {
        let floor = parse_crate_version(MIN_COMPATIBLE_VERSION);
        assert!(
            REPAIRED_CHECK_FLOOR_CONFIRMED
                || !version_at_least(floor, REPAIRED_INDICES_MIN_VERSION),
            "min-compatible-version {floor:?} reaches REPAIRED_INDICES_MIN_VERSION \
             {REPAIRED_INDICES_MIN_VERSION:?}, which switches visited-peers lookups to \
             the repaired bloom indices. If releases before it have left the network, \
             set REPAIRED_CHECK_FLOOR_CONFIRMED = true; otherwise lower the floor."
        );
    }

    /// Mirrors `ack_version_floor_tracks_the_shipping_release`: a release bump
    /// that lifts the crate to [`REPAIRED_INDICES_MIN_VERSION`] fails here until
    /// the releaser either records that it ships (`Some(floor)`) or raises the
    /// floor. A floor naming a release without the repaired marking would let
    /// the check switch on while peers of that release mark only the old bits.
    #[test]
    fn repaired_indices_floor_tracks_the_shipping_release() {
        let crate_version = parse_crate_version(crate::config::PCK_VERSION);
        match REPAIRED_INDICES_SHIPPED_IN {
            Some(shipped) => assert_eq!(
                shipped, REPAIRED_INDICES_MIN_VERSION,
                "the release that ships the repaired marking must EQUAL its floor"
            ),
            None => assert!(
                crate_version < REPAIRED_INDICES_MIN_VERSION,
                "floor {REPAIRED_INDICES_MIN_VERSION:?} is a PREDICTION about the next \
                 release, but the crate is already at {crate_version:?}: either set \
                 REPAIRED_INDICES_SHIPPED_IN to Some(floor) (this release ships it) or \
                 raise the floor"
            ),
        }
    }

    /// Companion to the marker test, which cannot catch the floor being
    /// lowered once `REPAIRED_INDICES_SHIPPED_IN` is `Some`. If the floor is
    /// raised because a release shipped without the repaired marking, move
    /// `LAST_RELEASE_WITHOUT` to that release too.
    #[test]
    fn repaired_indices_floor_stays_above_every_release_without_it() {
        /// Last release that marks only the unrepaired indices.
        const LAST_RELEASE_WITHOUT: (u8, u8, u16) = (0, 2, 150);
        assert!(REPAIRED_INDICES_MIN_VERSION > LAST_RELEASE_WITHOUT);
    }

    /// Covers every residue of `h2` modulo `BLOOM_BITS`: only the two
    /// degenerate ones may change, and those only by adding bits.
    #[test]
    fn repair_changes_only_degenerate_steps_and_only_by_adding_bits() {
        for residue in 0..BLOOM_BITS as u64 {
            for high in [0u64, 1, 0xdead_beef, u64::MAX >> 9] {
                let h1 = 0x5151_u64.wrapping_mul(residue + 7);
                let h2 = (high << 9) | residue;
                let old = [0u64, 1, 2, 3]
                    .map(|i| (h1.wrapping_add(h2.wrapping_mul(i)) as usize) % BLOOM_BITS);
                let new = VisitedPeers::repair_degenerate_step(old, h2);
                if residue % (BLOOM_BITS as u64 / 2) != 0 {
                    assert_eq!(new, old, "h2={h2:#x}");
                } else {
                    assert_eq!(distinct(&new), NUM_HASHES, "h2={h2:#x}: {new:?}");
                    assert!(old.iter().all(|b| new.contains(b)), "h2={h2:#x}");
                }
            }
        }
    }

    /// The measurement in #5994: two addresses marked, two other candidates
    /// checked, over many transactions. Keys are deterministic, so the count is
    /// fixed for a given hasher (a hasher change fails
    /// `hash_indices_match_previous_releases` first). The unrepaired check
    /// counts 201 false positives; the repaired one counts 63, mostly
    /// whole-set collisions (see the `VisitedPeers` docs). The threshold sits
    /// several standard deviations from both.
    #[test]
    fn small_set_false_positive_rate_has_no_degenerate_excess() {
        let marked: [SocketAddr; 2] = [
            "10.0.0.1:31337".parse().unwrap(),
            "10.0.0.2:31337".parse().unwrap(),
        ];
        let candidates: [SocketAddr; 2] = [
            "10.0.0.3:31337".parse().unwrap(),
            "10.0.0.4:31337".parse().unwrap(),
        ];
        let mut false_positives = 0;
        for i in 0..2_000_000u64 {
            let mut visited = with_keys(
                i.wrapping_mul(0x9e37_79b9_7f4a_7c15),
                i.wrapping_mul(0xbf58_476d_1ce4_e5b9) ^ GOLDEN_KEY1,
            );
            for addr in marked {
                visited.mark_visited(addr);
            }
            false_positives += candidates
                .iter()
                .filter(|addr| visited.probably_visited_with(**addr, true))
                .count();
        }
        assert!(
            false_positives < 120,
            "{false_positives} false positives in 4,000,000 checks"
        );
    }

    #[test]
    fn test_basic_operations() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        // Initially not visited
        assert!(!visited.probably_visited(addr));

        // After marking, should be visited
        visited.mark_visited(addr);
        assert!(visited.probably_visited(addr));

        // Different address should not be visited
        let other_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        assert!(!visited.probably_visited(other_addr));
    }

    #[test]
    fn test_multiple_addresses() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);

        let addrs: Vec<SocketAddr> = (8000..8020)
            .map(|port| format!("127.0.0.1:{}", port).parse().unwrap())
            .collect();

        // Mark all addresses
        for addr in &addrs {
            visited.mark_visited(*addr);
        }

        // All should be probably visited
        for addr in &addrs {
            assert!(
                visited.probably_visited(*addr),
                "Address {} should be visited",
                addr
            );
        }
    }

    #[test]
    fn test_transaction_isolation() {
        let tx1 = test_transaction();
        let tx2 = test_transaction();

        let v1 = VisitedPeers::new(&tx1);
        let v2 = VisitedPeers::new(&tx2);

        // Different transactions MUST use different hash keys (deterministic property)
        assert_ne!(
            v1.hash_keys, v2.hash_keys,
            "Different transactions must produce different hash keys"
        );

        // Same transaction should produce identical hash keys (deterministic)
        let v1_again = VisitedPeers::new(&tx1);
        assert_eq!(
            v1.hash_keys, v1_again.hash_keys,
            "Same transaction must produce identical hash keys"
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);

        let addrs: Vec<SocketAddr> = vec![
            "127.0.0.1:8000".parse().unwrap(),
            "192.168.1.1:9000".parse().unwrap(),
            "[::1]:8080".parse().unwrap(),
        ];

        for addr in &addrs {
            visited.mark_visited(*addr);
        }

        // Serialize and deserialize
        let bytes = bincode::serialize(&visited).expect("serialization failed");
        let deserialized: VisitedPeers =
            bincode::deserialize(&bytes).expect("deserialization failed");

        // Restore hash keys from transaction (required after deserialization)
        let deserialized = deserialized.with_transaction(&tx);

        // All marked addresses should still be visited
        for addr in &addrs {
            assert!(
                deserialized.probably_visited(*addr),
                "Address {} should be visited after roundtrip",
                addr
            );
        }
    }

    #[test]
    fn test_size_is_fixed() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);

        // Size should be fixed regardless of how many addresses we add
        let initial_size = std::mem::size_of_val(&visited.bits);
        assert_eq!(initial_size, BLOOM_BYTES);

        for port in 8000..8100 {
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            visited.mark_visited(addr);
        }

        let final_size = std::mem::size_of_val(&visited.bits);
        assert_eq!(final_size, BLOOM_BYTES);
    }

    #[test]
    fn test_false_positive_rate() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);

        // Insert 20 addresses (simulating HTL=20 worst case)
        let inserted_addrs: Vec<SocketAddr> = (8000..8020)
            .map(|port| format!("127.0.0.1:{}", port).parse().unwrap())
            .collect();

        for addr in &inserted_addrs {
            visited.mark_visited(*addr);
        }

        // Check 10,000 random addresses that were NOT inserted
        // Using more samples for statistical significance
        let mut false_positives = 0;
        for port in 10000..20000 {
            let addr: SocketAddr = format!("10.0.0.1:{}", port).parse().unwrap();
            if visited.probably_visited(addr) {
                false_positives += 1;
            }
        }

        // With 20 elements in 512-bit filter with k=4, the measured FP rate is
        // ~0.09% (see the `VisitedPeers` docs). Allow up to 0.5% (50 out of
        // 10,000) for test stability.
        let fp_rate = false_positives as f64 / 10000.0 * 100.0;
        assert!(
            false_positives <= 50,
            "False positive rate too high: {}/10000 = {:.3}% (expected ~0.04%)",
            false_positives,
            fp_rate
        );
    }

    #[test]
    fn test_no_false_negatives() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);

        // Insert many addresses
        let addrs: Vec<SocketAddr> = (8000..8050)
            .map(|port| format!("127.0.0.1:{}", port).parse().unwrap())
            .collect();

        for addr in &addrs {
            visited.mark_visited(*addr);
        }

        // Every inserted address MUST be detected (no false negatives)
        for addr in &addrs {
            assert!(
                visited.probably_visited(*addr),
                "False negative detected for {}",
                addr
            );
        }
    }

    #[test]
    fn test_serialized_size() {
        let tx = test_transaction();
        let visited = VisitedPeers::new(&tx);

        // Serialize with bincode
        let bytes = bincode::serialize(&visited).expect("serialization failed");

        // Expected size: 64 bytes (bits only, hash_keys are skipped)
        // bincode adds a small length prefix for the array
        assert_eq!(
            bytes.len(),
            64,
            "Serialized size should be exactly 64 bytes, got {}",
            bytes.len()
        );

        // Verify the size is fixed regardless of content
        let mut visited_full = VisitedPeers::new(&tx);
        for port in 8000..8100 {
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            visited_full.mark_visited(addr);
        }
        let bytes_full = bincode::serialize(&visited_full).expect("serialization failed");
        assert_eq!(
            bytes.len(),
            bytes_full.len(),
            "Serialized size should be fixed regardless of marked peers"
        );
    }

    /// Test documenting the correct behavior for request forwarding.
    ///
    /// When a node receives a forwarded GET/SUBSCRIBE request, it MUST mark BOTH:
    /// 1. Its own address (this_peer) - so requests won't be routed back to it
    /// 2. The sender's address (source_addr) - so requests won't cycle back to the sender
    ///
    /// This is critical for preventing request cycles, especially in scenarios where
    /// a node has limited connections (e.g., only connected to a gateway).
    ///
    /// Bug reproduction scenario (before fix):
    /// 1. Node A (originator, single connection to gateway) sends GET request
    /// 2. Gateway marks itself in visited, forwards to other peers
    /// 3. Other peers mark themselves, but NOT the sender
    /// 4. Eventually, some peer routes back to Node A (not in visited filter)
    /// 5. Node A receives its own request, can't forward (only peer is gateway, already in filter)
    /// 6. Request fails with "no upstream" error
    #[test]
    fn test_forwarding_must_mark_both_this_peer_and_sender() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);

        // Simulate the forwarding scenario
        let this_peer: SocketAddr = "10.0.0.1:8000".parse().unwrap();
        let sender: SocketAddr = "10.0.0.2:8000".parse().unwrap();
        let originator: SocketAddr = "10.0.0.3:8000".parse().unwrap();

        // When processing a forwarded request, mark BOTH this_peer AND sender
        visited.mark_visited(this_peer);
        visited.mark_visited(sender);

        // Both should be marked to prevent routing back to them
        assert!(
            visited.probably_visited(this_peer),
            "this_peer must be marked to prevent routing back to processing node"
        );
        assert!(
            visited.probably_visited(sender),
            "sender must be marked to prevent routing back to request source"
        );

        // The originator is still valid for routing (until they receive and mark themselves)
        // This documents that the originator should also be marked at the START of the operation
        assert!(
            !visited.probably_visited(originator),
            "originator not marked yet - they should mark themselves when initiating"
        );
    }

    /// Test that simulates the full routing cycle prevention.
    ///
    /// This tests the complete chain of marking that should prevent request cycles:
    /// Originator -> Gateway -> Peer1 -> Peer2 -> ... -> back to originator (should be blocked)
    #[test]
    fn test_request_cycle_prevention() {
        let tx = test_transaction();
        let mut visited = VisitedPeers::new(&tx);

        // Simulate the routing chain:
        // Originator (A) -> Gateway (G) -> Peer1 (P1) -> Peer2 (P2)
        let originator: SocketAddr = "10.0.0.1:8000".parse().unwrap();
        let gateway: SocketAddr = "10.0.0.2:8000".parse().unwrap();
        let peer1: SocketAddr = "10.0.0.3:8000".parse().unwrap();
        let peer2: SocketAddr = "10.0.0.4:8000".parse().unwrap();

        // BUG CONTEXT: In the original bug, only this_peer was marked at each hop,
        // NOT the sender. This meant requests could cycle back to earlier nodes.
        // The fix ensures each node marks BOTH itself AND its sender.

        // Step 1: Gateway receives from originator
        // Gateway marks: itself (gateway) AND sender (originator)
        visited.mark_visited(gateway);
        visited.mark_visited(originator);

        // Step 2: Peer1 receives from gateway
        // Peer1 marks: itself (peer1) AND sender (gateway)
        visited.mark_visited(peer1);
        visited.mark_visited(gateway); // Already marked, but this is what the code does

        // Step 3: Peer2 receives from peer1
        // Peer2 marks: itself (peer2) AND sender (peer1)
        visited.mark_visited(peer2);
        visited.mark_visited(peer1); // Already marked, but this is what the code does

        // Now, all nodes in the chain should be marked
        assert!(
            visited.probably_visited(originator),
            "originator must be blocked"
        );
        assert!(visited.probably_visited(gateway), "gateway must be blocked");
        assert!(visited.probably_visited(peer1), "peer1 must be blocked");
        assert!(visited.probably_visited(peer2), "peer2 must be blocked");

        // A new peer (peer3) can still be routed to
        let peer3: SocketAddr = "10.0.0.5:8000".parse().unwrap();
        assert!(
            !visited.probably_visited(peer3),
            "unvisited peer should still be valid for routing"
        );
    }
}

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

use crate::message::Transaction;

/// Number of bits in the bloom filter (512 bits = 64 bytes).
const BLOOM_BITS: usize = 512;

/// Number of bytes in the bloom filter.
const BLOOM_BYTES: usize = BLOOM_BITS / 8;

/// Number of hash functions to use (double-hashing generates 4 from 2).
const NUM_HASHES: usize = 4;

/// A bloom filter for tracking visited peers during contract search operations.
///
/// Uses the transaction ID as a hash seed to prevent topology inference attacks.
/// An observer cannot correlate visited sets across different transactions.
///
/// # False Positive Rates (k=4 hash functions, m=512 bits)
/// - 10 peers: ~0.006% (1 in 17,000)
/// - 20 peers: ~0.04% (1 in 2,500)
/// - 30 peers: ~0.2% (1 in 500)
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
    pub fn mark_visited(&mut self, addr: SocketAddr) {
        for idx in self.hash_indices(&addr) {
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
        for idx in self.hash_indices(&addr) {
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
    fn hash_indices(&self, addr: &SocketAddr) -> [usize; NUM_HASHES] {
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
        [
            (h1 as usize) % BLOOM_BITS,
            (h1.wrapping_add(h2) as usize) % BLOOM_BITS,
            (h1.wrapping_add(h2.wrapping_mul(2)) as usize) % BLOOM_BITS,
            (h1.wrapping_add(h2.wrapping_mul(3)) as usize) % BLOOM_BITS,
        ]
    }
}

/// Implement Contains trait for use with k_closest_potentially_caching.
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

        // With 20 elements in 512-bit filter with k=4, expected FP rate is ~0.04%
        // Allow up to 0.5% (50 out of 10,000) for test stability - still 12x expected rate
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
}

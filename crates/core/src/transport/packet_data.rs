use crate::transport::connection_handler::MAX_PACKET_SIZE;
use crate::transport::crypto::TransportPublicKey;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::rand_core::SeedableRng;
use aes_gcm::aead::{Aead, AeadMutInPlace};
use aes_gcm::{AeadCore, Aes128Gcm};
use rand::prelude::SmallRng;
use rand::{thread_rng, Rng};
use std::cell::RefCell;

// todo: split this into type for handling inbound (encrypted)/outbound (decrypted) packets for clarity
pub(super) struct PacketData<const N: usize = MAX_PACKET_SIZE> {
    data: [u8; N],
    size: usize,
}

// This must be very fast, but doesn't need to be cryptographically secure.
thread_local! {
static RNG: RefCell<SmallRng> = RefCell::new(
    SmallRng::from_rng(thread_rng()).expect("failed to create RNG")
);
}

// These are the same as the AES-GCM 128 constants, but extracting them from Aes128Gcm
// as consts was awkward.
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;

impl<const N: usize> PacketData<N> {
    pub(super) fn encrypted_with_cipher(data: &[u8], cipher: &mut Aes128Gcm) -> Self {
        debug_assert!(data.len() <= MAX_PACKET_SIZE - NONCE_SIZE - TAG_SIZE);

        let nonce: [u8; NONCE_SIZE] = RNG.with(|rng| rng.borrow_mut().gen());

        let mut buffer = [0u8; N];
        buffer[..NONCE_SIZE].copy_from_slice(&nonce);

        // Encrypt the data in place
        let payload_length = data.len();
        buffer[NONCE_SIZE..NONCE_SIZE + payload_length].copy_from_slice(data);
        let tag = cipher
            .encrypt_in_place_detached(
                &nonce.into(),
                &[],
                &mut buffer[NONCE_SIZE..NONCE_SIZE + payload_length],
            )
            .unwrap();

        // Append the tag to the buffer
        buffer[NONCE_SIZE + payload_length..NONCE_SIZE + payload_length + TAG_SIZE]
            .copy_from_slice(tag.as_slice());

        Self {
            data: buffer,
            size: NONCE_SIZE + payload_length + TAG_SIZE,
        }
    }

    pub(super) fn encrypted_with_remote(data: &[u8], remote_key: &TransportPublicKey) -> Self {
        let encrypted_data: Vec<u8> = remote_key.encrypt(data);
        debug_assert!(encrypted_data.len() <= MAX_PACKET_SIZE);
        let mut data = [0; N];
        data.copy_from_slice(&encrypted_data[..]);
        Self {
            data,
            size: encrypted_data.len(),
        }
    }

    pub(super) fn send_data(&self) -> &[u8] {
        &self.data[..self.size]
    }

    pub(super) fn decrypt(&self, inbound_sym_key: &mut Aes128Gcm) -> Result<Self, aes_gcm::Error> {
        debug_assert!(self.data.len() >= NONCE_SIZE + TAG_SIZE);
        debug_assert!(self.data.len() <= MAX_PACKET_SIZE + NONCE_SIZE + TAG_SIZE);

        let nonce = GenericArray::from_slice(&self.data[..NONCE_SIZE]);
        // Adjusted to extract the tag from the end of the encrypted data
        let tag = GenericArray::from_slice(&self.data[self.size - TAG_SIZE..self.size]);
        let encrypted_data = &self.data[NONCE_SIZE..self.size - TAG_SIZE];
        let mut buffer = [0u8; N];
        let buffer_len = encrypted_data.len();
        buffer[..buffer_len].copy_from_slice(encrypted_data);

        inbound_sym_key.decrypt_in_place_detached(nonce, &[], &mut buffer[..buffer_len], tag)?;

        Ok(Self {
            data: buffer,
            size: buffer_len,
        })
    }

    const fn max_data_size() -> usize {
        MAX_PACKET_SIZE - NONCE_SIZE - TAG_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::aead::rand_core::RngCore;
    use aes_gcm::{Aes128Gcm, KeyInit};
    use rand::rngs::OsRng;

    #[test]
    fn test_encryption_decryption() {
        // Generate a random 128-bit (16 bytes) key
        let mut key = [0u8; 16];
        OsRng.fill_bytes(&mut key);

        // Create a key object for AES-GCM
        let key = GenericArray::from_slice(&key);

        // Create a new AES-128-GCM instance
        let mut cipher = Aes128Gcm::new(key);
        let original_data = b"Hello, world!";
        let packet_data: PacketData<MAX_PACKET_SIZE> =
            PacketData::encrypted_with_cipher(original_data, &mut cipher);

        // Ensure data is not plainly visible
        assert_ne!(packet_data.data[..packet_data.size], *original_data);

        test_decryption(packet_data, &mut cipher, original_data);
    }

    // Test encryption/decryption where message size is the maximum allowed
    #[test]
    fn test_encryption_decryption_max_size() {
        // Generate a random 128-bit (16 bytes) key
        let mut key = [0u8; 16];
        OsRng.fill_bytes(&mut key);

        // Create a key object for AES-GCM
        let key = GenericArray::from_slice(&key);

        // Create a new AES-128-GCM instance
        let mut cipher = Aes128Gcm::new(key);
        let original_data = [0u8; MAX_PACKET_SIZE - NONCE_SIZE - TAG_SIZE];
        let packet_data: PacketData<MAX_PACKET_SIZE> =
            PacketData::encrypted_with_cipher(&original_data, &mut cipher);

        // Ensure data is not plainly visible
        assert!(longest_common_subsequence(&packet_data.data, &original_data) < 10);

        test_decryption(packet_data, &mut cipher, &original_data);
    }

    fn test_decryption<T: AsRef<[u8]>>(
        packet_data: PacketData<MAX_PACKET_SIZE>,
        cipher: &mut Aes128Gcm,
        original_data: T,
    ) {
        match packet_data.decrypt(cipher) {
            Ok(decrypted_data) => {
                // Ensure decrypted data matches original
                assert_eq!(
                    &decrypted_data.data[..decrypted_data.size],
                    original_data.as_ref()
                );
            }
            Err(e) => panic!("Decryption failed with error: {:?}", e),
        }
    }

    fn longest_common_subsequence(a: &[u8], b: &[u8]) -> usize {
        let m = a.len();
        let n = b.len();

        // Initialize a 2D vector with zeros. The dimensions are (m+1) x (n+1).
        let mut dp = vec![vec![0; n + 1]; m + 1];

        // Iterate over each character in both sequences
        for i in 0..m {
            for j in 0..n {
                if a[i] == b[j] {
                    // If characters match, increment the count from the previous subsequence
                    dp[i + 1][j + 1] = dp[i][j] + 1;
                } else {
                    // Otherwise, the current state is the max of either omitting the current character
                    // from sequence 'a' or 'b'.
                    dp[i + 1][j + 1] = std::cmp::max(dp[i + 1][j], dp[i][j + 1]);
                }
            }
        }

        // The value in the bottom-right cell of the matrix is the length of the LCS
        dp[m][n]
    }
}

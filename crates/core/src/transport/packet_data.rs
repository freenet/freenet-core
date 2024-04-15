use std::marker::PhantomData;
use std::{cell::RefCell, sync::Arc};

use aes_gcm::{
    aead::{generic_array::GenericArray, rand_core::SeedableRng, AeadInPlace},
    Aes128Gcm,
};
use rand::{prelude::SmallRng, thread_rng, Rng};

use crate::transport::crypto::TransportPublicKey;

use super::crypto::TransportSecretKey;
use super::TransportError;

/// The maximum size of a received UDP packet, MTU typically is 1500
pub(in crate::transport) const MAX_PACKET_SIZE: usize = 1500 - UDP_HEADER_SIZE;

// These are the same as the AES-GCM 128 constants, but extracting them from Aes128Gcm
// as consts was awkward.
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;

pub(super) const MAX_DATA_SIZE: usize = MAX_PACKET_SIZE - NONCE_SIZE - TAG_SIZE;
const UDP_HEADER_SIZE: usize = 8;

thread_local! {
    // This must be very fast, but doesn't need to be cryptographically secure.
    static RNG: RefCell<SmallRng> = RefCell::new(
        SmallRng::from_rng(thread_rng()).expect("failed to create RNG")
    );
}

struct AssertSize<const N: usize>;

impl<const N: usize> AssertSize<N> {
    const OK: () = assert!(N <= MAX_PACKET_SIZE);
}

// trying to bypass limitations with const generic checks on where clauses
const fn _check_valid_size<const N: usize>() {
    #[allow(clippy::let_unit_value)]
    let () = AssertSize::<N>::OK;
}

#[derive(Clone)]
pub(super) struct PacketData<DT: Encryption, const N: usize = MAX_PACKET_SIZE> {
    data: [u8; N],
    pub size: usize,
    data_type: PhantomData<DT>,
}

pub(super) trait Encryption: Clone {}

/// Decrypted packet
#[derive(Clone, Copy)]
pub(super) struct Plaintext;

/// Packet is encrypted using symmetric crypto (most packets if not an intro packet)
#[derive(Clone, Copy)]
pub(super) struct SymmetricAES;

/// Packet is encrypted using assympetric crypto (typically an intro packet)
#[derive(Clone, Copy)]
pub(super) struct AssymetricRSA;

/// This is used when we don't know the encryption type of the packet, perhaps because we
/// haven't yet determined whether it is an intro packet.
#[derive(Clone, Copy)]
pub(super) struct UnknownEncryption;

impl Encryption for Plaintext {}
impl Encryption for SymmetricAES {}
impl Encryption for AssymetricRSA {}
impl Encryption for UnknownEncryption {}

pub(super) const fn packet_size<const DATA_SIZE: usize>() -> usize {
    DATA_SIZE + NONCE_SIZE + TAG_SIZE
}

fn internal_sym_decryption<const N: usize>(
    data: &[u8],
    size: usize,
    inbound_sym_key: &Aes128Gcm,
) -> Result<([u8; N], usize), aes_gcm::Error> {
    debug_assert!(data.len() >= NONCE_SIZE + TAG_SIZE);

    let nonce = GenericArray::from_slice(&data[..NONCE_SIZE]);
    // Adjusted to extract the tag from the end of the encrypted data
    let tag = GenericArray::from_slice(&data[size - TAG_SIZE..size]);
    let encrypted_data = &data[NONCE_SIZE..size - TAG_SIZE];
    let mut buffer = [0u8; N];
    let buffer_len = encrypted_data.len();
    buffer[..buffer_len].copy_from_slice(encrypted_data);

    inbound_sym_key.decrypt_in_place_detached(nonce, &[], &mut buffer[..buffer_len], tag)?;
    Ok((buffer, buffer_len))
}

impl<DT: Encryption, const N: usize> PacketData<DT, N> {
    pub(super) fn data(&self) -> &[u8] {
        &self.data[..self.size]
    }
}

impl<const N: usize> PacketData<SymmetricAES, N> {
    #[cfg(test)]
    pub(super) fn decrypt(
        &self,
        inbound_sym_key: &Aes128Gcm,
    ) -> Result<PacketData<SymmetricAES, N>, aes_gcm::Error> {
        let (buffer, buffer_len) =
            internal_sym_decryption::<N>(&self.data[..], self.size, inbound_sym_key)?;

        Ok(Self {
            data: buffer,
            size: buffer_len,
            data_type: PhantomData,
        })
    }

    pub fn prepared_send(self) -> Arc<[u8]> {
        self.data[..self.size].into()
    }
}

impl<const N: usize> PacketData<AssymetricRSA, N> {
    pub(super) fn encrypt_with_pubkey(data: &[u8], remote_key: &TransportPublicKey) -> Self {
        _check_valid_size::<N>();
        let encrypted_data: Vec<u8> = remote_key.encrypt(data);
        debug_assert!(encrypted_data.len() <= MAX_PACKET_SIZE);
        let mut data = [0; N];
        data[..encrypted_data.len()].copy_from_slice(&encrypted_data[..]);
        Self {
            data,
            size: encrypted_data.len(),
            data_type: PhantomData,
        }
    }

    pub fn preparef_send(self) -> Arc<[u8]> {
        self.data[..self.size].into()
    }
}

impl<const N: usize> PacketData<Plaintext, N> {
    pub fn from_buf_plain(buf: impl AsRef<[u8]>) -> Self {
        let mut data = [0; N];
        let buf = buf.as_ref();
        let size = buf.len();
        data[..size].copy_from_slice(buf);
        Self {
            size,
            data,
            data_type: PhantomData,
        }
    }

    pub(super) fn encrypt_symmetric(&self, cipher: &Aes128Gcm) -> PacketData<SymmetricAES, N> {
        _check_valid_size::<N>();
        debug_assert!(self.size <= MAX_DATA_SIZE);

        let nonce: [u8; NONCE_SIZE] = RNG.with(|rng| rng.borrow_mut().gen());

        let mut buffer = [0u8; N];
        buffer[..NONCE_SIZE].copy_from_slice(&nonce);

        // Encrypt the data in place
        let payload_length = self.size;
        buffer[NONCE_SIZE..NONCE_SIZE + payload_length].copy_from_slice(self.data());
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

        PacketData {
            data: buffer,
            size: NONCE_SIZE + payload_length + TAG_SIZE,
            data_type: PhantomData,
        }
    }
}

impl<const N: usize> PacketData<UnknownEncryption, N> {
    pub fn from_buf(buf: impl AsRef<[u8]>) -> Self {
        let mut data = [0; N];
        let buf = buf.as_ref();
        let size = buf.len();
        data[..size].copy_from_slice(buf);
        Self {
            size,
            data,
            data_type: PhantomData,
        }
    }

    pub(super) fn is_intro_packet(
        &self,
        actual_intro_packet: &PacketData<AssymetricRSA, N>,
    ) -> bool {
        self.size == actual_intro_packet.size
            && self.data[..self.size] == actual_intro_packet.data[..actual_intro_packet.size]
    }

    pub(super) fn try_decrypt_sym(
        &self,
        inbound_sym_key: &Aes128Gcm,
    ) -> Result<PacketData<SymmetricAES, N>, aes_gcm::Error> {
        let (buffer, buffer_len) =
            internal_sym_decryption::<N>(&self.data[..], self.size, inbound_sym_key)?;

        Ok(PacketData {
            data: buffer,
            size: buffer_len,
            data_type: PhantomData,
        })
    }

    pub fn try_decrypt_asym(
        &self,
        key: &TransportSecretKey,
    ) -> Result<PacketData<AssymetricRSA, N>, TransportError> {
        let r = key.decrypt(self.data()).map(|decrypted| {
            let mut data = [0; N];
            data[..decrypted.len()].copy_from_slice(&decrypted[..]);
            PacketData {
                size: data.len(),
                data,
                data_type: PhantomData,
            }
        })?;
        Ok(r)
    }

    pub fn assert_assymetric(&self) -> PacketData<AssymetricRSA, N> {
        PacketData {
            data: self.data,
            size: self.size,
            data_type: PhantomData,
        }
    }
}

impl<DT: Encryption, const N: usize> Eq for PacketData<DT, N> {}

impl<DT: Encryption, const N: usize> PartialEq for PacketData<DT, N> {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size && self.data[..self.size] == other.data[..other.size]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::aead::rand_core::RngCore;
    use aes_gcm::KeyInit;
    use rand::rngs::OsRng;

    #[test]
    fn test_encryption_decryption() {
        // Generate a random 128-bit (16 bytes) key
        let mut key = [0u8; 16];
        OsRng.fill_bytes(&mut key);

        // Create a key object for AES-GCM
        let key = GenericArray::from_slice(&key);

        // Create a new AES-128-GCM instance
        let cipher = Aes128Gcm::new(key);
        let data = b"Hello, world!";
        let unencrypted_packet = PacketData::<_, 1000>::from_buf_plain(data);
        let encrypted_packet = unencrypted_packet.encrypt_symmetric(&cipher);

        let _overlap = longest_common_subsequence(&encrypted_packet.data, data.as_slice());

        test_decryption(encrypted_packet, &cipher, unencrypted_packet);
    }

    // Test detection of packet corruption
    #[test]
    fn test_encryption_decryption_corrupted() {
        // Generate a random 128-bit (16 bytes) key
        let mut key = [0u8; 16];
        OsRng.fill_bytes(&mut key);

        // Create a key object for AES-GCM
        let key = GenericArray::from_slice(&key);

        // Create a new AES-128-GCM instance
        let cipher = Aes128Gcm::new(key);
        let data = b"Hello, world!";
        let unencrypted_packet = PacketData::<_, 1000>::from_buf_plain(data);
        let mut encrypted_packet = unencrypted_packet.encrypt_symmetric(&cipher);

        // Corrupt the packet data
        encrypted_packet.data[encrypted_packet.size / 2] = 0;

        // Ensure decryption fails
        match encrypted_packet.decrypt(&cipher) {
            Ok(_) => panic!("Decryption succeeded when it should have failed"),
            Err(e) => assert_eq!(e, aes_gcm::Error),
        }
    }

    fn test_decryption<const N: usize>(
        packet_data: PacketData<SymmetricAES, N>,
        cipher: &Aes128Gcm,
        original_data: PacketData<Plaintext, N>,
    ) {
        match packet_data.decrypt(cipher) {
            Ok(decrypted_data) => {
                // Ensure decrypted data matches original
                assert_eq!(&decrypted_data.data(), &original_data.data());
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
        for (i, _) in a.iter().enumerate() {
            for (j, _) in b.iter().enumerate() {
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

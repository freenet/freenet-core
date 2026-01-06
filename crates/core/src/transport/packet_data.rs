use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use aes_gcm::{aead::AeadInPlace, Aes128Gcm};

use crate::config::GlobalRng;
use crate::transport::crypto::{
    TransportPublicKey, PACKET_TYPE_INTRO, PACKET_TYPE_SIZE, PACKET_TYPE_SYMMETRIC,
};

use super::crypto::TransportSecretKey;
use super::TransportError;

/// The maximum size of a received UDP packet, MTU typically is 1500
pub(in crate::transport) const MAX_PACKET_SIZE: usize = 1500 - UDP_HEADER_SIZE;

// These are the same as the AES-GCM 128 constants, but extracting them from Aes128Gcm
// as consts was awkward.
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;

/// Maximum plaintext data size that can be encrypted into a symmetric packet.
/// Accounts for packet type (1) + nonce (12) + tag (16) overhead.
pub(super) const MAX_DATA_SIZE: usize = MAX_PACKET_SIZE - PACKET_TYPE_SIZE - NONCE_SIZE - TAG_SIZE;
const UDP_HEADER_SIZE: usize = 8;

/// Counter-based nonce generation for AES-GCM.
/// Uses 8 bytes from an atomic counter + 4 random bytes generated at startup.
/// This is ~5.5x faster than random nonce generation while maintaining uniqueness.
static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Random prefix generated lazily to ensure nonce uniqueness across process restarts.
/// Resettable for deterministic simulation testing.
static NONCE_RANDOM_PREFIX: Mutex<Option<[u8; 4]>> = Mutex::new(None);

/// Get or generate the nonce random prefix.
fn get_nonce_prefix() -> [u8; 4] {
    let mut guard = NONCE_RANDOM_PREFIX.lock().unwrap();
    if let Some(prefix) = *guard {
        prefix
    } else {
        let mut bytes = [0u8; 4];
        GlobalRng::fill_bytes(&mut bytes);
        *guard = Some(bytes);
        bytes
    }
}

/// Reset the nonce counter and prefix to initial state.
/// Used for deterministic simulation testing.
/// Call this AFTER GlobalRng::set_seed() so the prefix is regenerated
/// deterministically on next use.
pub fn reset_nonce_counter() {
    NONCE_COUNTER.store(0, Ordering::SeqCst);
    *NONCE_RANDOM_PREFIX.lock().unwrap() = None;
}

/// Generate a unique 12-byte nonce using counter + random prefix.
/// This is faster than random generation while ensuring uniqueness.
#[inline]
fn generate_nonce() -> [u8; NONCE_SIZE] {
    let counter = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut nonce = [0u8; NONCE_SIZE];
    // First 4 bytes: random prefix (ensures uniqueness across restarts)
    nonce[..4].copy_from_slice(&get_nonce_prefix());
    // Last 8 bytes: counter (ensures uniqueness within this process)
    nonce[4..].copy_from_slice(&counter.to_le_bytes());
    nonce
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
pub(crate) struct PacketData<DT: Encryption, const N: usize = MAX_PACKET_SIZE> {
    data: [u8; N],
    pub size: usize,
    data_type: PhantomData<DT>,
}

pub(crate) trait Encryption: Clone {}

/// Decrypted packet
#[derive(Clone, Copy)]
pub(crate) struct Plaintext;

/// Packet is encrypted using symmetric crypto (most packets if not an intro packet)
#[derive(Clone, Copy)]
pub(crate) struct SymmetricAES;

/// Packet is encrypted using asymmetric crypto (typically an intro packet)
/// Uses X25519 static-ephemeral key exchange with ChaCha20Poly1305
#[derive(Clone, Copy)]
pub(super) struct AsymmetricX25519;

/// This is used when we don't know the encryption type of the packet, perhaps because we
/// haven't yet determined whether it is an intro packet.
#[derive(Clone, Copy)]
pub(crate) struct UnknownEncryption;

impl Encryption for Plaintext {}
impl Encryption for SymmetricAES {}
impl Encryption for AsymmetricX25519 {}
impl Encryption for UnknownEncryption {}

fn internal_sym_decryption<const N: usize>(
    data: &[u8],
    size: usize,
    inbound_sym_key: &Aes128Gcm,
) -> Result<([u8; N], usize), aes_gcm::Error> {
    debug_assert!(data.len() >= PACKET_TYPE_SIZE + NONCE_SIZE + TAG_SIZE);

    // Check packet type (first byte should be PACKET_TYPE_SYMMETRIC)
    if data[0] != PACKET_TYPE_SYMMETRIC {
        return Err(aes_gcm::Error);
    }

    let nonce = (&data[PACKET_TYPE_SIZE..PACKET_TYPE_SIZE + NONCE_SIZE]).into();
    // Adjusted to extract the tag from the end of the encrypted data
    let tag = (&data[size - TAG_SIZE..size]).into();
    let encrypted_data = &data[PACKET_TYPE_SIZE + NONCE_SIZE..size - TAG_SIZE];
    let mut buffer = [0u8; N];
    let buffer_len = encrypted_data.len();
    buffer[..buffer_len].copy_from_slice(encrypted_data);

    inbound_sym_key.decrypt_in_place_detached(nonce, &[], &mut buffer[..buffer_len], tag)?;
    Ok((buffer, buffer_len))
}

impl<DT: Encryption, const N: usize> PacketData<DT, N> {
    pub(crate) fn data(&self) -> &[u8] {
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

    pub fn prepared_send(self) -> Box<[u8]> {
        self.data[..self.size].into()
    }
}

impl<const N: usize> PacketData<AsymmetricX25519, N> {
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

    pub(crate) fn encrypt_symmetric(&self, cipher: &Aes128Gcm) -> PacketData<SymmetricAES, N> {
        _check_valid_size::<N>();
        debug_assert!(self.size <= MAX_DATA_SIZE);

        let nonce = generate_nonce();

        let mut buffer = [0u8; N];
        // Prepend packet type
        buffer[0] = PACKET_TYPE_SYMMETRIC;
        buffer[PACKET_TYPE_SIZE..PACKET_TYPE_SIZE + NONCE_SIZE].copy_from_slice(&nonce);

        // Encrypt the data in place
        let payload_length = self.size;
        buffer[PACKET_TYPE_SIZE + NONCE_SIZE..PACKET_TYPE_SIZE + NONCE_SIZE + payload_length]
            .copy_from_slice(self.data());
        let tag = cipher
            .encrypt_in_place_detached(
                &nonce.into(),
                &[],
                &mut buffer
                    [PACKET_TYPE_SIZE + NONCE_SIZE..PACKET_TYPE_SIZE + NONCE_SIZE + payload_length],
            )
            .unwrap();

        // Append the tag to the buffer
        buffer[PACKET_TYPE_SIZE + NONCE_SIZE + payload_length
            ..PACKET_TYPE_SIZE + NONCE_SIZE + payload_length + TAG_SIZE]
            .copy_from_slice(&tag);

        PacketData {
            data: buffer,
            size: PACKET_TYPE_SIZE + NONCE_SIZE + payload_length + TAG_SIZE,
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

    /// Get the packet type discriminator from the first byte.
    /// Returns None if the packet is too small or has an invalid packet type.
    pub(super) fn packet_type(&self) -> Option<u8> {
        if self.size < PACKET_TYPE_SIZE {
            return None;
        }
        let packet_type = self.data[0];
        if packet_type == PACKET_TYPE_INTRO || packet_type == PACKET_TYPE_SYMMETRIC {
            Some(packet_type)
        } else {
            None
        }
    }

    /// Check if this is an intro packet by examining the packet type discriminator.
    pub(super) fn is_intro_packet(&self) -> bool {
        self.packet_type() == Some(PACKET_TYPE_INTRO)
    }

    /// Check if this is a symmetric packet by examining the packet type discriminator.
    #[allow(dead_code)] // Provides API completeness, may be used in future
    pub(super) fn is_symmetric_packet(&self) -> bool {
        self.packet_type() == Some(PACKET_TYPE_SYMMETRIC)
    }

    pub(crate) fn try_decrypt_sym(
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

    pub(super) fn try_decrypt_asym(
        &self,
        key: &TransportSecretKey,
    ) -> Result<PacketData<AsymmetricX25519, N>, TransportError> {
        let decrypted = key.decrypt(self.data())?;
        let mut data = [0; N];
        data[..decrypted.len()].copy_from_slice(&decrypted[..]);
        Ok(PacketData {
            size: decrypted.len(),
            data,
            data_type: PhantomData,
        })
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

    use aes_gcm::KeyInit;

    #[test]
    fn test_encryption_decryption() {
        // Generate a random 128-bit (16 bytes) key
        let mut key = [0u8; 16];
        GlobalRng::fill_bytes(&mut key);

        // Create a key object for AES-GCM
        let key = (&key).into();

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
        GlobalRng::fill_bytes(&mut key);

        // Create a key object for AES-GCM
        let key = (&key).into();

        // Create a new AES-128-GCM instance
        let cipher = Aes128Gcm::new(key);
        let data = b"Hello, world!";
        let unencrypted_packet = PacketData::<_, 1000>::from_buf_plain(data);
        let mut encrypted_packet = unencrypted_packet.encrypt_symmetric(&cipher);

        // Corrupt the packet data by flipping bits at a deterministic position.
        let mid = encrypted_packet.size / 2;
        encrypted_packet.data[mid] ^= 0xFF;

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
            Err(e) => panic!("Decryption failed with error: {e:?}"),
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

#[cfg(test)]
mod packet_type_discrimination_tests {
    use super::*;
    use crate::transport::crypto::TransportKeypair;
    use aes_gcm::KeyInit;

    #[test]
    fn test_is_intro_packet_with_valid_intro() {
        let keypair = TransportKeypair::new();
        let data = b"test intro packet data";
        let encrypted = keypair.public().encrypt(data);

        let packet = PacketData::<UnknownEncryption, MAX_PACKET_SIZE>::from_buf(&encrypted);

        assert!(
            packet.is_intro_packet(),
            "Should identify valid intro packet"
        );
        assert!(
            !packet.is_symmetric_packet(),
            "Intro packet should not be identified as symmetric"
        );
        assert_eq!(packet.packet_type(), Some(PACKET_TYPE_INTRO));
    }

    #[test]
    fn test_is_symmetric_packet_with_valid_symmetric() {
        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());
        let plaintext = PacketData::<Plaintext, 1000>::from_buf_plain(b"test symmetric data");
        let encrypted = plaintext.encrypt_symmetric(&cipher);

        let unknown = PacketData::<UnknownEncryption, 1000>::from_buf(encrypted.data());

        assert!(
            unknown.is_symmetric_packet(),
            "Should identify valid symmetric packet"
        );
        assert!(
            !unknown.is_intro_packet(),
            "Symmetric packet should not be identified as intro"
        );
        assert_eq!(unknown.packet_type(), Some(PACKET_TYPE_SYMMETRIC));
    }

    #[test]
    fn test_packet_type_with_invalid_type_byte() {
        let invalid_packet = [0xFFu8; 100]; // Invalid type 0xFF
        let packet = PacketData::<UnknownEncryption, MAX_PACKET_SIZE>::from_buf(invalid_packet);

        assert_eq!(
            packet.packet_type(),
            None,
            "Should return None for invalid packet type"
        );
        assert!(
            !packet.is_intro_packet(),
            "Invalid type should not be intro"
        );
        assert!(
            !packet.is_symmetric_packet(),
            "Invalid type should not be symmetric"
        );
    }

    #[test]
    fn test_packet_type_with_empty_packet() {
        let empty: [u8; 0] = [];
        let packet = PacketData::<UnknownEncryption, MAX_PACKET_SIZE>::from_buf(empty);

        assert_eq!(
            packet.packet_type(),
            None,
            "Empty packet should return None"
        );
        assert!(!packet.is_intro_packet());
        assert!(!packet.is_symmetric_packet());
    }

    #[test]
    fn test_max_data_size_fits_in_max_packet_size() {
        // Create maximum size plaintext
        let max_plaintext = vec![0xAB; MAX_DATA_SIZE];
        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());

        let plaintext = PacketData::<Plaintext, MAX_PACKET_SIZE>::from_buf_plain(&max_plaintext);
        let encrypted = plaintext.encrypt_symmetric(&cipher);

        assert!(
            encrypted.size <= MAX_PACKET_SIZE,
            "Encrypted packet ({} bytes) should not exceed MAX_PACKET_SIZE ({} bytes). \
             MAX_DATA_SIZE may need adjustment.",
            encrypted.size,
            MAX_PACKET_SIZE
        );
    }

    #[test]
    fn test_symmetric_encryption_includes_packet_type() {
        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());
        let plaintext = PacketData::<Plaintext, 1000>::from_buf_plain(b"test data");
        let encrypted = plaintext.encrypt_symmetric(&cipher);

        assert_eq!(
            encrypted.data()[0],
            PACKET_TYPE_SYMMETRIC,
            "First byte should be PACKET_TYPE_SYMMETRIC (0x02)"
        );
    }

    #[test]
    fn test_symmetric_decryption_validates_packet_type() {
        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());
        let plaintext = PacketData::<Plaintext, 1000>::from_buf_plain(b"test");
        let mut encrypted = plaintext.encrypt_symmetric(&cipher);

        // Corrupt packet type
        encrypted.data[0] = PACKET_TYPE_INTRO; // Wrong type

        let unknown = PacketData::<UnknownEncryption, 1000>::from_buf(encrypted.data());
        let result = unknown.try_decrypt_sym(&cipher);

        assert!(
            result.is_err(),
            "Decryption should fail when packet type is wrong"
        );
    }

    #[test]
    fn test_packet_type_values() {
        assert_eq!(PACKET_TYPE_INTRO, 0x01, "PACKET_TYPE_INTRO should be 0x01");
        assert_eq!(
            PACKET_TYPE_SYMMETRIC, 0x02,
            "PACKET_TYPE_SYMMETRIC should be 0x02"
        );
        assert_eq!(PACKET_TYPE_SIZE, 1, "PACKET_TYPE_SIZE should be 1 byte");
    }
}

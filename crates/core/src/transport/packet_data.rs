use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use aes_gcm::{aead::AeadInPlace, Aes128Gcm};
use once_cell::sync::Lazy;
use rand::{rng, Rng};

use crate::transport::crypto::TransportPublicKey;

/// The maximum size of a received UDP packet, MTU typically is 1500
pub(in crate::transport) const MAX_PACKET_SIZE: usize = 1500 - UDP_HEADER_SIZE;

// These are the same as the AES-GCM 128 constants, but extracting them from Aes128Gcm
// as consts was awkward.
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;

pub(super) const MAX_DATA_SIZE: usize = MAX_PACKET_SIZE - NONCE_SIZE - TAG_SIZE;
const UDP_HEADER_SIZE: usize = 8;

/// Size of X25519 intro packets: protocol version (variable) + ephemeral public key (32 bytes).
/// For version "0.1.74" (7 bytes) + 32 = 39 bytes.
/// We use a range check rather than exact size since protocol version length varies.
pub(super) const X25519_PUBLIC_KEY_SIZE: usize = 32;

/// Counter-based nonce generation for AES-GCM.
/// Uses 8 bytes from an atomic counter + 4 random bytes generated at startup.
/// This is ~5.5x faster than random nonce generation while maintaining uniqueness.
static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Random prefix generated once at startup to ensure nonce uniqueness across process restarts.
static NONCE_RANDOM_PREFIX: Lazy<[u8; 4]> = Lazy::new(|| rng().random());

/// Generate a unique 12-byte nonce using counter + random prefix.
/// This is faster than random generation while ensuring uniqueness.
#[inline]
fn generate_nonce() -> [u8; NONCE_SIZE] {
    let counter = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut nonce = [0u8; NONCE_SIZE];
    // First 4 bytes: random prefix (ensures uniqueness across restarts)
    nonce[..4].copy_from_slice(&*NONCE_RANDOM_PREFIX);
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

/// X25519 intro packet containing ephemeral public key for key exchange.
/// Unlike RSA, this packet is NOT encrypted - it contains a public key that
/// the responder uses for ECDH key derivation.
#[derive(Clone, Copy)]
pub(super) struct IntroPacket;

/// This is used when we don't know the encryption type of the packet, perhaps because we
/// haven't yet determined whether it is an intro packet.
#[derive(Clone, Copy)]
pub(crate) struct UnknownEncryption;

impl Encryption for Plaintext {}
impl Encryption for SymmetricAES {}
impl Encryption for IntroPacket {}
impl Encryption for UnknownEncryption {}

fn internal_sym_decryption<const N: usize>(
    data: &[u8],
    size: usize,
    inbound_sym_key: &Aes128Gcm,
) -> Result<([u8; N], usize), aes_gcm::Error> {
    debug_assert!(data.len() >= NONCE_SIZE + TAG_SIZE);

    let nonce = (&data[..NONCE_SIZE]).into();
    // Adjusted to extract the tag from the end of the encrypted data
    let tag = (&data[size - TAG_SIZE..size]).into();
    let encrypted_data = &data[NONCE_SIZE..size - TAG_SIZE];
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

impl<const N: usize> PacketData<IntroPacket, N> {
    /// Create an X25519 intro packet with protocol version and ephemeral public key.
    /// The packet is NOT encrypted - it's just the public key for ECDH.
    pub(super) fn create_intro(
        protoc_version: &[u8],
        ephemeral_public: &TransportPublicKey,
    ) -> Self {
        _check_valid_size::<N>();
        let mut data = [0; N];
        let version_len = protoc_version.len();
        data[..version_len].copy_from_slice(protoc_version);
        data[version_len..version_len + X25519_PUBLIC_KEY_SIZE]
            .copy_from_slice(ephemeral_public.as_bytes());
        Self {
            data,
            size: version_len + X25519_PUBLIC_KEY_SIZE,
            data_type: PhantomData,
        }
    }

    /// Get the raw bytes of the intro packet for sending.
    #[allow(dead_code)]
    pub fn prepared_send(&self) -> Box<[u8]> {
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

    pub(crate) fn encrypt_symmetric(&self, cipher: &Aes128Gcm) -> PacketData<SymmetricAES, N> {
        _check_valid_size::<N>();
        debug_assert!(self.size <= MAX_DATA_SIZE);

        let nonce = generate_nonce();

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
            .copy_from_slice(&tag);

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

    pub(super) fn is_intro_packet(&self, actual_intro_packet: &PacketData<IntroPacket, N>) -> bool {
        self.size == actual_intro_packet.size
            && self.data[..self.size] == actual_intro_packet.data[..actual_intro_packet.size]
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

    /// Try to parse as an X25519 intro packet containing protocol version + ephemeral public key.
    /// Returns the ephemeral public key if the packet matches the expected format.
    pub(super) fn try_parse_intro(
        &self,
        expected_protoc_version: &[u8],
    ) -> Option<TransportPublicKey> {
        let version_len = expected_protoc_version.len();
        let expected_size = version_len + X25519_PUBLIC_KEY_SIZE;

        if self.size != expected_size {
            return None;
        }

        // Check protocol version
        if &self.data[..version_len] != expected_protoc_version {
            return None;
        }

        // Extract public key
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&self.data[version_len..version_len + X25519_PUBLIC_KEY_SIZE]);
        Some(TransportPublicKey::from_bytes(key_bytes))
    }

    /// Check if this could be an intro packet based on size.
    /// Used for quick filtering before attempting full validation.
    #[allow(dead_code)]
    pub(super) fn could_be_intro_packet(&self, protoc_version_len: usize) -> bool {
        self.size == protoc_version_len + X25519_PUBLIC_KEY_SIZE
    }

    pub(super) fn assert_intro(&self) -> PacketData<IntroPacket, N> {
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

    use aes_gcm::KeyInit;

    #[test]
    fn test_encryption_decryption() {
        // Generate a random 128-bit (16 bytes) key
        let mut key = [0u8; 16];
        rand::rng().fill(&mut key);

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
        rand::rng().fill(&mut key);

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

    #[test]
    fn test_intro_packet_creation_and_parsing() {
        use crate::transport::crypto::TransportKeypair;

        let keypair = TransportKeypair::new();
        let protoc_version = b"0.1.74";

        // Create intro packet
        let intro: PacketData<IntroPacket, MAX_PACKET_SIZE> =
            PacketData::create_intro(protoc_version, keypair.public());

        // Verify size: version (6 bytes) + public key (32 bytes)
        assert_eq!(intro.size, 6 + 32);

        // Parse as unknown packet
        let unknown: PacketData<UnknownEncryption, MAX_PACKET_SIZE> =
            PacketData::from_buf(intro.data());

        // Verify we can parse the public key back
        let parsed_key = unknown.try_parse_intro(protoc_version).unwrap();
        assert_eq!(parsed_key.as_bytes(), keypair.public().as_bytes());
    }

    #[test]
    fn test_intro_packet_wrong_version_fails() {
        use crate::transport::crypto::TransportKeypair;

        let keypair = TransportKeypair::new();
        let protoc_version = b"0.1.74";

        let intro: PacketData<IntroPacket, MAX_PACKET_SIZE> =
            PacketData::create_intro(protoc_version, keypair.public());

        let unknown: PacketData<UnknownEncryption, MAX_PACKET_SIZE> =
            PacketData::from_buf(intro.data());

        // Wrong version should fail
        assert!(unknown.try_parse_intro(b"0.1.75").is_none());
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

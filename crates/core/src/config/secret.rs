use std::path::Path;

use aes_gcm::KeyInit;
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use freenet_stdlib::client_api::DelegateRequest;

use super::*;

const NONCE_SIZE: usize = 24;
const CIPHER_SIZE: usize = 32;

impl ConfigArgs {
    pub(super) fn read_secrets(
        path_to_key: Option<PathBuf>,
        path_to_nonce: Option<PathBuf>,
        path_to_cipher: Option<PathBuf>,
    ) -> std::io::Result<Secrets> {
        let transport_keypair = if let Some(ref path_to_key) = path_to_key {
            read_transport_keypair(path_to_key)?
        } else {
            TransportKeypair::new()
        };
        let nonce = if let Some(ref path_to_nonce) = path_to_nonce {
            read_nonce(path_to_nonce)?
        } else {
            DelegateRequest::DEFAULT_NONCE
        };
        let cipher = if let Some(ref path_to_cipher) = path_to_cipher {
            read_cipher(path_to_cipher)?
        } else {
            DelegateRequest::DEFAULT_CIPHER
        };

        Ok(Secrets {
            transport_keypair,
            transport_keypair_path: path_to_key,
            nonce,
            nonce_path: path_to_nonce,
            cipher,
            cipher_path: path_to_cipher,
        })
    }
}

#[derive(Debug, Default, Clone, clap::Parser, serde::Serialize, serde::Deserialize)]
pub struct SecretArgs {
    /// Path to the RSA private key for the transport layer.
    #[clap(long, value_parser, default_value=None, env = "TRANSPORT_KEYPAIR")]
    pub transport_keypair: Option<PathBuf>,

    /// Path to the nonce file for encrypting data.
    #[clap(long, value_parser, default_value=None, env = "NONCE")]
    pub nonce: Option<PathBuf>,

    /// Path to the cipher file for encrypting data.
    #[clap(long, value_parser, default_value=None, env = "CIPHER")]
    pub cipher: Option<PathBuf>,
}

impl SecretArgs {
    pub(super) fn build(self) -> std::io::Result<Secrets> {
        let transport_key = self
            .transport_keypair
            .as_ref()
            .map(read_transport_keypair)
            .transpose()?;
        let (transport_keypair_path, transport_keypair) = if let Some(transport_key) = transport_key
        {
            (self.transport_keypair, transport_key)
        } else {
            let transport_key = TransportKeypair::new();
            (None, transport_key)
        };
        let nonce = self.nonce.as_ref().map(read_nonce).transpose()?;
        let (nonce_path, nonce) = if let Some(nonce) = nonce {
            (self.nonce, nonce)
        } else {
            (None, DelegateRequest::DEFAULT_NONCE)
        };

        let cipher = self.cipher.as_ref().map(read_cipher).transpose()?;

        let (cipher_path, cipher) = if let Some(cipher) = cipher {
            (self.cipher, cipher)
        } else {
            (None, DelegateRequest::DEFAULT_CIPHER)
        };

        Ok(Secrets {
            transport_keypair,
            transport_keypair_path,
            nonce,
            nonce_path,
            cipher,
            cipher_path,
        })
    }

    pub(super) fn merge(&mut self, other: Secrets) {
        if self.transport_keypair.is_none() {
            self.transport_keypair = other.transport_keypair_path;
        }

        if self.nonce.is_none() {
            self.nonce = other.nonce_path;
        }

        if self.cipher.is_none() {
            self.cipher = other.cipher_path;
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Secrets {
    #[serde(skip)]
    pub transport_keypair: TransportKeypair,
    #[serde(rename = "transport_keypair", skip_serializing_if = "Option::is_none")]
    pub transport_keypair_path: Option<PathBuf>,
    #[serde(skip)]
    pub nonce: [u8; 24],
    #[serde(rename = "nonce", skip_serializing_if = "Option::is_none")]
    pub nonce_path: Option<PathBuf>,
    #[serde(skip)]
    pub cipher: [u8; 32],
    #[serde(rename = "cipher", skip_serializing_if = "Option::is_none")]
    pub cipher_path: Option<PathBuf>,
}

// Only used in tests
#[cfg(test)]
impl Default for Secrets {
    fn default() -> Self {
        let transport_keypair = TransportKeypair::new();
        let nonce = DelegateRequest::DEFAULT_NONCE;
        let cipher = DelegateRequest::DEFAULT_CIPHER;

        Secrets {
            transport_keypair,
            transport_keypair_path: None,
            nonce,
            nonce_path: None,
            cipher,
            cipher_path: None,
        }
    }
}

impl Secrets {
    #[inline]
    pub fn nonce(&self) -> XNonce {
        self.nonce.into()
    }

    #[inline]
    pub fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new((&self.cipher).into())
    }

    #[inline]
    pub fn transport_keypair(&self) -> &TransportKeypair {
        &self.transport_keypair
    }
}

fn read_nonce(path_to_nonce: impl AsRef<Path>) -> std::io::Result<[u8; NONCE_SIZE]> {
    let path_to_nonce = path_to_nonce.as_ref();
    let mut nonce_file = File::open(path_to_nonce).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to open key file {}: {e}", path_to_nonce.display()),
        )
    })?;
    let mut buf = [0u8; NONCE_SIZE];
    nonce_file.read_exact(&mut buf).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to read key file {}: {e}", path_to_nonce.display()),
        )
    })?;

    Ok::<_, std::io::Error>(buf)
}

fn read_cipher(path_to_cipher: impl AsRef<Path>) -> std::io::Result<[u8; CIPHER_SIZE]> {
    let path_to_cipher = path_to_cipher.as_ref();
    let mut cipher_file = File::open(path_to_cipher).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to open key file {}: {e}", path_to_cipher.display()),
        )
    })?;
    let mut buf = [0u8; CIPHER_SIZE];
    cipher_file.read_exact(&mut buf).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to read key file {}: {e}", path_to_cipher.display()),
        )
    })?;

    Ok::<_, std::io::Error>(buf)
}

fn read_transport_keypair(path_to_key: impl AsRef<Path>) -> std::io::Result<TransportKeypair> {
    TransportKeypair::load(path_to_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_from_different_files() {
        let transport_keypair = TransportKeypair::new();
        let nonce = [0u8; NONCE_SIZE];
        let cipher = [0u8; CIPHER_SIZE];

        let transport_keypair_file = tempfile::NamedTempFile::new().unwrap();
        let mut nonce_file = tempfile::NamedTempFile::new().unwrap();
        let mut cipher_file = tempfile::NamedTempFile::new().unwrap();

        // write secrets to files using hex format (new X25519 format)
        transport_keypair
            .save(transport_keypair_file.path())
            .unwrap();
        nonce_file.write_all(&nonce).unwrap();
        cipher_file.write_all(&cipher).unwrap();

        let secrets = Secrets {
            transport_keypair,
            transport_keypair_path: Some(transport_keypair_file.path().to_path_buf()),
            nonce,
            nonce_path: Some(nonce_file.path().to_path_buf()),
            cipher,
            cipher_path: Some(cipher_file.path().to_path_buf()),
        };

        let secret_args = SecretArgs {
            transport_keypair: Some(transport_keypair_file.path().to_path_buf()),
            nonce: Some(nonce_file.path().to_path_buf()),
            cipher: Some(cipher_file.path().to_path_buf()),
        };

        let loaded_secrets = secret_args.build().unwrap();
        assert_eq!(secrets, loaded_secrets);
    }

    #[test]
    fn test_load_default() {
        let secret_args = SecretArgs::default();
        let loaded_secrets = secret_args.build().unwrap();
        assert_eq!(DelegateRequest::DEFAULT_CIPHER, loaded_secrets.cipher);
        assert_eq!(DelegateRequest::DEFAULT_NONCE, loaded_secrets.nonce);
    }
}

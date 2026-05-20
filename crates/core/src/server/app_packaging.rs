//! Helper functions and types for dealing with HTTP client API compatible contracts.
use std::{
    io::{Cursor, Read},
    path::{Component, Path},
};
use tracing::{debug, instrument};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tar::{Archive, Builder};
use xz2::read::{XzDecoder, XzEncoder};

#[derive(Debug, thiserror::Error)]
pub enum WebContractError {
    #[error("unpacking error: {0}")]
    UnpackingError(anyhow::Error),
    #[error("{0}")]
    StoringError(std::io::Error),
    #[error("file not found: {0}")]
    FileNotFound(String),
}

#[non_exhaustive]
pub struct WebApp {
    pub metadata: Vec<u8>,
    pub web: Vec<u8>,
}

impl WebApp {
    #[instrument(level = "debug", skip(web))]
    pub fn from_data(
        metadata: Vec<u8>,
        web: Builder<Cursor<Vec<u8>>>,
    ) -> Result<Self, WebContractError> {
        debug!("Creating WebApp from metadata ({} bytes)", metadata.len());
        let buf = web.into_inner().unwrap().into_inner();
        let mut encoder = XzEncoder::new(Cursor::new(buf), 6);
        let mut compressed = vec![];
        encoder.read_to_end(&mut compressed).unwrap();
        Ok(Self {
            metadata,
            web: compressed,
        })
    }

    pub fn from_compressed(
        metadata: Vec<u8>,
        compressed_web: Vec<u8>,
    ) -> Result<Self, WebContractError> {
        debug!(
            "Creating WebApp with metadata size {} bytes and pre-compressed web content {} bytes",
            metadata.len(),
            compressed_web.len()
        );
        Ok(Self {
            metadata,
            web: compressed_web,
        })
    }

    pub fn pack(mut self) -> std::io::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(
            self.metadata.len() + self.web.len() + (std::mem::size_of::<u64>() * 2),
        );
        output.write_u64::<BigEndian>(self.metadata.len() as u64)?;
        output.append(&mut self.metadata);
        output.write_u64::<BigEndian>(self.web.len() as u64)?;
        output.append(&mut self.web);
        Ok(output)
    }

    #[instrument(level = "debug", skip(self, dst))]
    pub fn unpack(&mut self, dst: impl AsRef<Path>) -> Result<(), WebContractError> {
        let dst = dst.as_ref();
        debug!("Unpacking web content to {:?}", dst);
        std::fs::create_dir_all(dst).map_err(WebContractError::StoringError)?;
        let dst = dst.canonicalize().map_err(WebContractError::StoringError)?;

        let mut decoded_web = self.decode_web();
        decoded_web.set_overwrite(false);
        decoded_web.set_preserve_mtime(false);
        for entry in decoded_web
            .entries()
            .map_err(WebContractError::StoringError)?
        {
            let mut entry = entry.map_err(WebContractError::StoringError)?;
            // Reject entries that would escape `dst`: an absolute path, or
            // one with a `..` component. A malicious contract could otherwise
            // ship an archive that writes outside the cache directory.
            let path = entry
                .path()
                .map_err(WebContractError::StoringError)?
                .into_owned();
            if path.is_absolute() || path.components().any(|c| c == Component::ParentDir) {
                return Err(WebContractError::UnpackingError(anyhow::anyhow!(
                    "archive entry escapes the destination directory: {path:?}"
                )));
            }
            entry
                .unpack_in(&dst)
                .map_err(WebContractError::StoringError)?;
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    pub fn get_file(&mut self, path: &str) -> Result<Vec<u8>, WebContractError> {
        debug!("Retrieving file from web content: {}", path);
        let mut decoded_web = self.decode_web();
        for e in decoded_web
            .entries()
            .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?
        {
            let mut e = e.map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;
            if e.path()
                .ok()
                .filter(|p| p.to_string_lossy() == path)
                .is_some()
            {
                let mut bytes = vec![];
                e.read_to_end(&mut bytes)
                    .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;
                return Ok(bytes);
            }
        }
        Err(WebContractError::FileNotFound(path.to_owned()))
    }

    fn decode_web(&self) -> Archive<XzDecoder<&[u8]>> {
        debug!("Decoding compressed web content ({} bytes)", self.web.len());
        let decoder = XzDecoder::new(self.web.as_slice());
        let mut archive = Archive::new(decoder);

        // Debug log the archive contents
        match archive.entries() {
            Ok(entries) => {
                debug!("Archive contents:");
                for entry in entries.flatten() {
                    if let Ok(path) = entry.path() {
                        debug!("  {}", path.display());
                    }
                }
            }
            Err(e) => debug!("Failed to read archive entries: {}", e),
        }

        // Create a fresh archive since we consumed the entries
        Archive::new(XzDecoder::new(self.web.as_slice()))
    }
}

impl<'a> TryFrom<&'a [u8]> for WebApp {
    type Error = WebContractError;

    fn try_from(state: &'a [u8]) -> Result<Self, Self::Error> {
        debug!(
            "Attempting to create WebApp from {} bytes of state",
            state.len()
        );
        const MAX_METADATA_SIZE: u64 = 1024;
        const MAX_WEB_SIZE: u64 = 1024 * 1024 * 100;
        // Decompose the state and extract the compressed web interface
        let mut state = Cursor::new(state);

        let metadata_size = state
            .read_u64::<BigEndian>()
            .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;
        if metadata_size > MAX_METADATA_SIZE {
            return Err(WebContractError::UnpackingError(anyhow::anyhow!(
                "Exceeded metadata size of 1kB: {} bytes",
                metadata_size
            )));
        }
        let mut metadata = vec![0; metadata_size as usize];
        state
            .read_exact(&mut metadata)
            .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;

        let web_size = state
            .read_u64::<BigEndian>()
            .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;
        if web_size > MAX_WEB_SIZE {
            return Err(WebContractError::UnpackingError(anyhow::anyhow!(
                "Exceeded packed web size of 100MB: {} bytes",
                web_size
            )));
        }
        let mut web = vec![0; web_size as usize];
        state
            .read_exact(&mut web)
            .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;

        Ok(Self { metadata, web })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tar::Header;

    fn web_app(entries: &[(&str, &[u8])]) -> WebApp {
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        for (name, data) in entries {
            let mut header = Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            // Write the name field directly: Builder::append_data rejects a
            // `..` path, but a malicious or non-Rust tar will happily include
            // one, which is exactly the input this guard defends against.
            let name_bytes = name.as_bytes();
            header.as_gnu_mut().unwrap().name[..name_bytes.len()].copy_from_slice(name_bytes);
            header.set_cksum();
            builder
                .append(&header, *data)
                .expect("append archive entry");
        }
        WebApp::from_data(Vec::new(), builder).expect("build WebApp")
    }

    #[test]
    fn unpack_writes_a_benign_archive() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = web_app(&[("index.html", b"<html></html>")]);
        app.unpack(dir.path())
            .expect("benign archive should unpack");
        assert!(dir.path().join("index.html").exists());
    }

    #[test]
    fn unpack_rejects_parent_dir_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = web_app(&[("../escape.txt", b"pwned")]);
        let err = app
            .unpack(dir.path().join("web"))
            .expect_err("an archive entry with a `..` component must be rejected");
        assert!(matches!(err, WebContractError::UnpackingError(_)));
        // The malicious entry must not have escaped into the parent directory.
        assert!(!dir.path().join("escape.txt").exists());
    }

    #[test]
    fn unpack_rejects_absolute_path_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = web_app(&[("/etc/passwd", b"pwned")]);
        let err = app
            .unpack(dir.path())
            .expect_err("an archive entry with an absolute path must be rejected");
        assert!(matches!(err, WebContractError::UnpackingError(_)));
    }
}

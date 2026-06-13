//! Helper functions and types for dealing with HTTP client API compatible contracts.
use std::{
    io::{Cursor, Read},
    path::Path,
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

    /// Unpacks the web archive into `dst`, confining every written file to that
    /// directory.
    ///
    /// Web contracts are authored by untrusted third parties, so the embedded
    /// tar archive is adversarial input. A malicious author can hand-craft tar
    /// bytes whose entry paths contain `..`, are absolute, or are symlinks /
    /// hardlinks pointing outside `dst`; left unchecked, those entries let the
    /// archive write arbitrary files on the node operator's filesystem (CWE-22
    /// path traversal / zip-slip). See freenet/freenet-core#3946.
    ///
    /// We therefore validate each entry ourselves and **reject** (rather than
    /// silently skip) anything that could escape the destination:
    ///
    /// - absolute paths,
    /// - any component equal to `..` (`ParentDir`),
    /// - symlink and hardlink entries (web apps have no legitimate need for
    ///   them, and they are the primary symlink-escape vector).
    ///
    /// Overwrite is disabled so a republished contract cannot clobber files
    /// already present at the destination via a crafted duplicate entry.
    #[instrument(level = "debug", skip(self, dst))]
    pub fn unpack(&mut self, dst: impl AsRef<Path>) -> Result<(), WebContractError> {
        use std::path::Component;
        use tar::EntryType;

        let dst = dst.as_ref();
        debug!("Unpacking web content to {:?}", dst);

        let mut decoded_web = self.decode_web();
        decoded_web.set_overwrite(false);
        decoded_web.set_preserve_mtime(false);

        let entries = decoded_web
            .entries()
            .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;

        for entry in entries {
            let mut entry =
                entry.map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;

            // Reject link entries outright: a symlink/hardlink whose target is
            // outside `dst` (or a symlink followed by a later write-through
            // entry) is the classic zip-slip escape, and a web app never needs
            // links on disk.
            let entry_type = entry.header().entry_type();
            if entry_type == EntryType::Symlink || entry_type == EntryType::Link {
                let path = entry.path().map(|p| p.to_path_buf()).unwrap_or_default();
                return Err(WebContractError::UnpackingError(anyhow::anyhow!(
                    "refusing to unpack link entry from web archive: {path:?}"
                )));
            }

            let path = entry
                .path()
                .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;

            // Reject absolute paths and any `..` component before touching the
            // filesystem. `tar`'s own `unpack_in` would merely *skip* such
            // entries; we want a hard error so a malicious archive is visible
            // to the operator instead of partially extracting.
            if path.is_absolute()
                || path
                    .components()
                    .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
            {
                return Err(WebContractError::UnpackingError(anyhow::anyhow!(
                    "path traversal attempt in web archive entry: {path:?}"
                )));
            }

            // `unpack_in` performs an additional canonicalization-based
            // containment check as defense in depth and returns the destination
            // it would write into.
            entry
                .unpack_in(dst)
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
    use tar::{Builder, EntryType, Header};

    /// Append a regular-file entry whose in-header path is written from raw
    /// bytes, deliberately bypassing `tar::Builder`'s append-time path
    /// validation.
    ///
    /// `tar::Builder::append_data` rejects paths containing `..` or absolute
    /// paths, so a benign packer can never *produce* a malicious archive
    /// through the high-level API. A real attacker, however, writes the tar
    /// bytes directly with any tool, so the header name field can contain
    /// arbitrary bytes. Writing the name field directly here reproduces that
    /// adversarial archive faithfully.
    fn append_raw_path_file(
        builder: &mut Builder<Cursor<Vec<u8>>>,
        raw_path: &str,
        payload: &[u8],
    ) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_size(payload.len() as u64);
        // Write the path straight into the old-header name field, skipping the
        // `set_path` validation that would otherwise reject `..`/absolute.
        let name_bytes = raw_path.as_bytes();
        let name_field = &mut header.as_old_mut().name;
        name_field.fill(0);
        name_field[..name_bytes.len()].copy_from_slice(name_bytes);
        // Checksum must be recomputed after mutating the header in place.
        header.set_cksum();
        builder.append(&header, payload).unwrap();
    }

    /// Append a symlink entry whose target points wherever the caller wants
    /// (including outside the destination).
    fn append_symlink(builder: &mut Builder<Cursor<Vec<u8>>>, link_name: &str, target: &str) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Symlink);
        header.set_size(0);
        builder.append_link(&mut header, link_name, target).unwrap();
    }

    fn finish(builder: Builder<Cursor<Vec<u8>>>) -> WebApp {
        WebApp::from_data(b"meta".to_vec(), builder).unwrap()
    }

    #[test]
    fn unpack_rejects_parent_dir_traversal() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        append_raw_path_file(&mut builder, "../escape.txt", b"pwned");
        let mut web = finish(builder);

        let result = web.unpack(dst.path());

        // The escaping entry must be rejected with an error, not silently
        // skipped: a partial unpack that swallows traversal entries hides
        // the attack from the operator.
        assert!(
            result.is_err(),
            "unpack must reject an archive entry containing `..`"
        );
        // And nothing may be written outside the destination directory.
        let escaped = dst.path().parent().unwrap().join("escape.txt");
        assert!(
            !escaped.exists(),
            "path-traversal entry escaped the destination: {escaped:?}"
        );
    }

    #[test]
    fn unpack_rejects_absolute_path() {
        let dst = tempfile::tempdir().unwrap();
        // An absolute target the attacker hopes to overwrite.
        let abs_target = dst.path().parent().unwrap().join("abs_escape.txt");
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        append_raw_path_file(&mut builder, abs_target.to_str().unwrap(), b"pwned");
        let mut web = finish(builder);

        let result = web.unpack(dst.path());

        assert!(
            result.is_err(),
            "unpack must reject an archive entry with an absolute path"
        );
        assert!(
            !abs_target.exists(),
            "absolute-path entry escaped the destination: {abs_target:?}"
        );
    }

    #[test]
    fn unpack_rejects_escaping_symlink() {
        let dst = tempfile::tempdir().unwrap();
        // Symlink whose target points outside the destination. A follow-up
        // entry writing "through" this link is the classic zip-slip escalation;
        // rejecting the symlink itself closes the door before that can happen.
        let outside = dst.path().parent().unwrap().join("symlink_target");
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        append_symlink(&mut builder, "link", outside.to_str().unwrap());
        let mut web = finish(builder);

        let result = web.unpack(dst.path());

        assert!(
            result.is_err(),
            "unpack must reject symlink entries to prevent symlink-escape writes"
        );
        assert!(
            !dst.path().join("link").exists(),
            "symlink entry was created despite pointing outside the destination"
        );
    }

    #[test]
    fn unpack_accepts_legitimate_nested_entries() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        let mut h = Header::new_gnu();
        h.set_entry_type(EntryType::Regular);
        h.set_size(b"<html></html>".len() as u64);
        h.set_cksum();
        builder
            .append_data(&mut h, "index.html", b"<html></html>" as &[u8])
            .unwrap();
        let mut h = Header::new_gnu();
        h.set_entry_type(EntryType::Regular);
        h.set_size(b"console.log(1)".len() as u64);
        h.set_cksum();
        builder
            .append_data(&mut h, "assets/app.js", b"console.log(1)" as &[u8])
            .unwrap();
        let mut web = finish(builder);

        web.unpack(dst.path())
            .expect("a well-formed web archive must still unpack");

        assert_eq!(
            std::fs::read(dst.path().join("index.html")).unwrap(),
            b"<html></html>"
        );
        assert_eq!(
            std::fs::read(dst.path().join("assets/app.js")).unwrap(),
            b"console.log(1)"
        );
    }
}

//! Helper functions and types for dealing with HTTP client API compatible contracts.
use std::{
    io::{Cursor, Read},
    path::{Path, PathBuf},
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

/// Largest compressed web archive accepted from a contract's state.
const MAX_WEB_SIZE: u64 = 1024 * 1024 * 100;

/// Most bytes a web archive may unpack to, summed over all entries.
///
/// `MAX_WEB_SIZE` bounds only the compressed bytes, and xz compresses runs of
/// zeros by several orders of magnitude, so it says nothing about what lands on
/// disk or in memory. This bound is below the node's webapp cache budget
/// (`WEBAPP_CACHE_MAX_BYTES`, 64 MiB; a test in `path_handlers` pins the
/// relation) so a single archive can never exceed the budget by itself, and well
/// above the largest real web app we have measured (about 23 MB unpacked, 14.5 MB
/// of it one wasm file).
pub(crate) const MAX_UNPACKED_TOTAL_BYTES: u64 = 48 * 1024 * 1024;

/// Largest single file in a web archive. Twice the largest wasm bundle measured
/// (14.5 MB), and below the total bound.
pub(crate) const MAX_UNPACKED_FILE_BYTES: u64 = 32 * 1024 * 1024;

/// Most entries (files plus directories) in a web archive. Real apps have tens
/// to a few hundred.
pub(crate) const MAX_ARCHIVE_ENTRIES: usize = 4096;

/// Per-entry allowance for tar headers, extension records and padding when
/// bounding the raw decompressed stream.
const TAR_ENTRY_OVERHEAD_BYTES: u64 = 4096;

/// Most decompressed bytes read from the xz stream, headers and padding
/// included. Backstops the per-entry checks: whatever the tar headers claim,
/// the decoder is never asked for more than this.
const MAX_DECOMPRESSED_STREAM_BYTES: u64 =
    MAX_UNPACKED_TOTAL_BYTES + MAX_ARCHIVE_ENTRIES as u64 * TAR_ENTRY_OVERHEAD_BYTES;

/// A reader that fails (rather than reporting a clean EOF) once more than
/// `remaining` bytes have been read.
struct LimitedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            // At the limit: any further byte is an error, a clean EOF is fine.
            let mut probe = [0u8; 1];
            return match self.inner.read(&mut probe)? {
                0 => Ok(0),
                _ => Err(std::io::Error::other(
                    "web archive exceeds the decompressed size limit",
                )),
            };
        }
        let max = self.remaining.min(buf.len() as u64) as usize;
        let n = self.inner.read(&mut buf[..max])?;
        self.remaining -= n as u64;
        Ok(n)
    }
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
    /// silently skip) anything that could escape the destination or exhaust the
    /// node's resources:
    ///
    /// - every entry type other than regular files and directories (links,
    ///   GNU sparse files, devices, FIFOs, ...),
    /// - absolute paths and any component equal to `..` (`ParentDir`),
    /// - more than [`MAX_ARCHIVE_ENTRIES`] entries, any file larger than
    ///   [`MAX_UNPACKED_FILE_BYTES`], or more than [`MAX_UNPACKED_TOTAL_BYTES`]
    ///   in total. The decoder itself is also capped, so the limits hold
    ///   whatever the tar headers claim.
    ///
    /// Overwrite is disabled so a republished contract cannot clobber files
    /// already present at the destination via a crafted duplicate entry. On any
    /// error, everything this call created under `dst` is removed again.
    #[instrument(level = "debug", skip(self, dst))]
    pub fn unpack(&mut self, dst: impl AsRef<Path>) -> Result<(), WebContractError> {
        let dst = dst.as_ref();
        debug!("Unpacking web content to {:?}", dst);

        let mut created: Vec<PathBuf> = Vec::new();
        let result = self.unpack_checked(dst, &mut created);
        if result.is_err() {
            for path in created.iter().rev() {
                let removed = if path.is_dir() {
                    std::fs::remove_dir_all(path)
                } else {
                    std::fs::remove_file(path)
                };
                if let Err(e) = removed {
                    debug!("could not remove partial web archive output {path:?}: {e}");
                }
            }
        }
        result
    }

    fn unpack_checked(
        &mut self,
        dst: &Path,
        created: &mut Vec<PathBuf>,
    ) -> Result<(), WebContractError> {
        use std::path::Component;
        use tar::EntryType;

        let reject = |msg: String| WebContractError::UnpackingError(anyhow::anyhow!(msg));

        let mut decoded_web = self.decode_web();
        decoded_web.set_overwrite(false);
        decoded_web.set_preserve_mtime(false);

        let entries = decoded_web
            .entries()
            .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;

        let mut total: u64 = 0;
        let mut count: usize = 0;
        for entry in entries {
            let mut entry =
                entry.map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;

            count += 1;
            if count > MAX_ARCHIVE_ENTRIES {
                return Err(reject(format!(
                    "web archive has more than {MAX_ARCHIVE_ENTRIES} entries"
                )));
            }

            // Allow-list: only regular files and directories. Links are the
            // zip-slip vector, and sparse files (typeflag `S`) materialise a
            // file far larger than their on-wire size.
            let entry_type = entry.header().entry_type();
            if !matches!(entry_type, EntryType::Regular | EntryType::Directory) {
                let path = entry.path().map(|p| p.to_path_buf()).unwrap_or_default();
                return Err(reject(format!(
                    "refusing to unpack {entry_type:?} entry from web archive: {path:?}"
                )));
            }

            let path = entry
                .path()
                .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?
                .into_owned();

            // Reject absolute paths and any `..` component before touching the
            // filesystem. `tar`'s own `unpack_in` would merely *skip* such
            // entries; we want a hard error so a malicious archive is visible
            // to the operator instead of partially extracting.
            if path.is_absolute()
                || path
                    .components()
                    .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
            {
                return Err(reject(format!(
                    "path traversal attempt in web archive entry: {path:?}"
                )));
            }

            // `entry.size()` is the size of the data that will actually be read
            // for this entry (it honours PAX overrides), and the entry reader
            // yields no more than that, so this is what gets written.
            let size = entry.size();
            if size > MAX_UNPACKED_FILE_BYTES {
                return Err(reject(format!(
                    "web archive entry {path:?} is {size} bytes, over the {MAX_UNPACKED_FILE_BYTES} byte limit"
                )));
            }
            total = total.saturating_add(size);
            if total > MAX_UNPACKED_TOTAL_BYTES {
                return Err(reject(format!(
                    "web archive unpacks to more than {MAX_UNPACKED_TOTAL_BYTES} bytes"
                )));
            }

            // Remember the first new top-level path so a failure can remove
            // what this call created without touching what was already in `dst`.
            if let Some(first) = path.components().find_map(|c| match c {
                Component::Normal(n) => Some(dst.join(n)),
                Component::Prefix(_)
                | Component::RootDir
                | Component::CurDir
                | Component::ParentDir => None,
            }) {
                if !first.exists() && !created.contains(&first) {
                    created.push(first);
                }
            }

            // `unpack_in` performs an additional canonicalization-based
            // containment check as defense in depth.
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
            let e = e.map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;
            if e.path()
                .ok()
                .filter(|p| p.to_string_lossy() == path)
                .is_some()
            {
                if e.size() > MAX_UNPACKED_FILE_BYTES {
                    return Err(WebContractError::UnpackingError(anyhow::anyhow!(
                        "web archive entry {path} is {} bytes, over the {MAX_UNPACKED_FILE_BYTES} byte limit",
                        e.size()
                    )));
                }
                let mut bytes = vec![];
                e.take(MAX_UNPACKED_FILE_BYTES)
                    .read_to_end(&mut bytes)
                    .map_err(|e| WebContractError::UnpackingError(anyhow::anyhow!(e)))?;
                return Ok(bytes);
            }
        }
        Err(WebContractError::FileNotFound(path.to_owned()))
    }

    fn decode_web(&self) -> Archive<LimitedReader<XzDecoder<&[u8]>>> {
        debug!("Decoding compressed web content ({} bytes)", self.web.len());
        Archive::new(LimitedReader {
            inner: XzDecoder::new(self.web.as_slice()),
            remaining: MAX_DECOMPRESSED_STREAM_BYTES,
        })
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

    // ---- resource-exhaustion limits ----------------------------------------

    /// A regular file of `len` zero bytes. xz shrinks this to a few KB, which is
    /// what makes the size limits (rather than `MAX_WEB_SIZE`) the only defence.
    fn append_zero_file(builder: &mut Builder<Cursor<Vec<u8>>>, name: &str, len: u64) {
        let mut h = Header::new_gnu();
        h.set_entry_type(EntryType::Regular);
        h.set_size(len);
        h.set_cksum();
        builder
            .append_data(&mut h, name, std::io::repeat(0).take(len))
            .unwrap();
    }

    /// A GNU sparse entry: 512 bytes on the wire, `real_size` bytes on disk.
    fn append_sparse(builder: &mut Builder<Cursor<Vec<u8>>>, name: &str, real_size: u64) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::GNUSparse);
        header.set_size(0);
        header.set_path(name).unwrap();
        let encoded = format!("{real_size:012o}");
        header
            .as_gnu_mut()
            .unwrap()
            .realsize
            .copy_from_slice(encoded.as_bytes());
        header.set_cksum();
        builder.append(&header, std::io::empty()).unwrap();
    }

    fn append_special(builder: &mut Builder<Cursor<Vec<u8>>>, name: &str, ty: EntryType) {
        let mut h = Header::new_gnu();
        h.set_entry_type(ty);
        h.set_size(0);
        h.set_path(name).unwrap();
        h.set_cksum();
        builder.append(&h, std::io::empty()).unwrap();
    }

    fn dir_is_empty(p: &Path) -> bool {
        std::fs::read_dir(p).unwrap().next().is_none()
    }

    #[test]
    fn unpack_rejects_gnu_sparse_entry() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        append_sparse(&mut builder, "huge.bin", 1 << 34);
        let mut web = finish(builder);

        assert!(
            web.unpack(dst.path()).is_err(),
            "sparse entry must be rejected"
        );
        assert!(
            !dst.path().join("huge.bin").exists(),
            "sparse entry was materialised"
        );
    }

    #[test]
    fn unpack_rejects_special_entry_types() {
        for ty in [
            EntryType::Fifo,
            EntryType::Char,
            EntryType::Block,
            EntryType::Continuous,
        ] {
            let dst = tempfile::tempdir().unwrap();
            let mut builder = Builder::new(Cursor::new(Vec::new()));
            append_special(&mut builder, "special", ty);
            let mut web = finish(builder);
            assert!(
                web.unpack(dst.path()).is_err(),
                "{ty:?} entry must be rejected"
            );
            assert!(dir_is_empty(dst.path()), "{ty:?} entry left output behind");
        }
    }

    #[test]
    fn unpack_rejects_file_over_per_file_limit() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        append_zero_file(&mut builder, "big.bin", MAX_UNPACKED_FILE_BYTES + 1);
        let mut web = finish(builder);
        assert!(
            (web.web.len() as u64) < 1024 * 1024,
            "fixture must be small compressed"
        );

        assert!(web.unpack(dst.path()).is_err());
        assert!(
            dir_is_empty(dst.path()),
            "oversized file left output behind"
        );
    }

    #[test]
    fn unpack_rejects_total_over_limit_with_each_file_under_per_file_limit() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        let each = MAX_UNPACKED_TOTAL_BYTES / 2 + 1024;
        assert!(each <= MAX_UNPACKED_FILE_BYTES);
        append_zero_file(&mut builder, "a.bin", each);
        append_zero_file(&mut builder, "b.bin", each);
        let mut web = finish(builder);

        assert!(web.unpack(dst.path()).is_err());
        assert!(
            dir_is_empty(dst.path()),
            "partial output was not cleaned up"
        );
    }

    #[test]
    fn unpack_rejects_too_many_entries() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        for i in 0..=MAX_ARCHIVE_ENTRIES {
            append_zero_file(&mut builder, &format!("f{i}"), 0);
        }
        let mut web = finish(builder);

        assert!(web.unpack(dst.path()).is_err());
        assert!(
            dir_is_empty(dst.path()),
            "partial output was not cleaned up"
        );
    }

    #[test]
    fn unpack_failure_removes_earlier_entries_but_keeps_preexisting_files() {
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(dst.path().join("keep.txt"), b"keep").unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        append_zero_file(&mut builder, "ok/index.html", 10);
        append_zero_file(&mut builder, "top.js", 10);
        append_sparse(&mut builder, "huge.bin", 1 << 34);
        let mut web = finish(builder);

        assert!(web.unpack(dst.path()).is_err());
        assert!(!dst.path().join("ok").exists());
        assert!(!dst.path().join("top.js").exists());
        assert_eq!(std::fs::read(dst.path().join("keep.txt")).unwrap(), b"keep");
    }

    #[test]
    fn unpack_accepts_a_realistic_large_app() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        // Shaped like the largest app measured: ~23 MB, 14.5 MB of it one wasm.
        append_zero_file(&mut builder, "assets/app_bg.wasm", 15 * 1024 * 1024);
        append_zero_file(&mut builder, "assets/app.js", 8 * 1024 * 1024);
        append_zero_file(&mut builder, "index.html", 1024);
        let mut web = finish(builder);

        web.unpack(dst.path()).expect("a realistic app must unpack");
        assert_eq!(
            std::fs::metadata(dst.path().join("assets/app_bg.wasm"))
                .unwrap()
                .len(),
            15 * 1024 * 1024
        );
    }

    #[test]
    fn get_file_rejects_entry_over_per_file_limit() {
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        append_zero_file(&mut builder, "big.bin", MAX_UNPACKED_FILE_BYTES + 1);
        let mut web = finish(builder);
        assert!(web.get_file("big.bin").is_err());
    }

    /// Extension records are not counted as file bytes, so only the cap on the
    /// decompressed stream bounds an archive that pads itself with them.
    #[test]
    fn unpack_rejects_archive_padded_with_extension_records() {
        let dst = tempfile::tempdir().unwrap();
        let mut builder = Builder::new(Cursor::new(Vec::new()));
        let padding = "x".repeat(100_000);
        // 100 KB per record, below tar's own extension-size limit, well under
        // the entry-count limit, and over the stream cap in total.
        let records = (MAX_DECOMPRESSED_STREAM_BYTES / 100_000 + 20) as usize;
        for i in 0..records {
            builder
                .append_pax_extensions([("comment", padding.as_bytes())])
                .unwrap();
            append_zero_file(&mut builder, &format!("f{i}"), 0);
        }
        let mut web = finish(builder);

        assert!(web.unpack(dst.path()).is_err());
        assert!(dir_is_empty(dst.path()));
    }

    #[test]
    fn limited_reader_errors_instead_of_truncating() {
        let mut r = LimitedReader {
            inner: std::io::repeat(7),
            remaining: 100,
        };
        let mut buf = vec![0u8; 4096];
        let mut total = 0usize;
        let err = loop {
            match r.read(&mut buf) {
                Ok(0) => panic!("limit must surface as an error, not EOF"),
                Ok(n) => total += n,
                Err(e) => break e,
            }
        };
        assert_eq!(total, 100);
        assert!(err.to_string().contains("limit"));
    }
}

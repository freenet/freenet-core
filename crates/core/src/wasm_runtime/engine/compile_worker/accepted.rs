//! The modules the worker has accepted, remembered across restarts.
//!
//! Without this file every restart would run the worker again for each hosted
//! module at its first use, although the node's own compile is then a hit in the
//! wasmtime disk cache. The file holds the blake3 hash of each accepted module
//! and the memory its compile needs (the worker's peak, with a margin), which
//! the node reserves again before every compile of that module in this process.
//! It is written only after the node's own compile of the module succeeded.
//!
//! Records of the earlier format (hash only) are read with an unknown
//! requirement, which is reserved as the ceiling, and the file is rewritten in
//! the current format.
//!
//! Integrity is file permissions: whoever can write this file can already write
//! the wasmtime cache directory. A wrong entry only skips the worker for one
//! module, which is the behaviour of a node without the worker.
//!
//! The file is tolerant of damage: a bad header, a size over the bound or a
//! partial last record leaves the set empty (or keeps the whole records before
//! the partial one). A file that is not a whole number of records, or that
//! holds a repeated hash, is rewritten from its valid records at open, so no
//! later append lands after stray bytes.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const MAGIC: [u8; 4] = *b"FNAM";
const FORMAT_VERSION: u32 = 2;
/// The earlier format: records of the hash alone.
const LEGACY_VERSION: u32 = 1;
const HEADER_BYTES: usize = 4 + 4 + 8 + 16;
const LEGACY_RECORD_BYTES: usize = 32;
/// Hash, then the memory requirement in MiB (little endian; 0 = unknown).
const RECORD_BYTES: usize = 32 + 4;
/// Most records kept. Over it, the newest half is rewritten.
pub(crate) const MAX_RECORDS: usize = 65_536;
/// Largest file read: the header plus a full file plus one record of slack.
const MAX_FILE_BYTES: u64 = (HEADER_BYTES + (MAX_RECORDS + 1) * RECORD_BYTES) as u64;

/// What makes an old acceptance stale: the engine that compiles and the caps the
/// worker judged under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stamp {
    pub engine_compat_id: u64,
    pub caps_digest: [u8; 16],
}

/// One accepted module: its hash and the memory its compile needs, in MiB
/// (0 when unknown).
type Record = ([u8; 32], u32);

struct Inner {
    set: HashMap<[u8; 32], u32>,
    /// Records in file order, so compaction can keep the newest half.
    order: Vec<Record>,
}

pub(crate) struct AcceptedModules {
    path: Option<PathBuf>,
    stamp: Stamp,
    inner: Mutex<Inner>,
}

fn header(stamp: &Stamp) -> Vec<u8> {
    header_v(stamp, FORMAT_VERSION)
}

fn header_v(stamp: &Stamp, version: u32) -> Vec<u8> {
    let mut h = Vec::with_capacity(HEADER_BYTES);
    h.extend_from_slice(&MAGIC);
    h.extend_from_slice(&version.to_le_bytes());
    h.extend_from_slice(&stamp.engine_compat_id.to_le_bytes());
    h.extend_from_slice(&stamp.caps_digest);
    h
}

/// File name for an engine identity: another engine uses another file.
pub(crate) fn file_name(engine_compat_id: u64) -> String {
    format!("{engine_compat_id:016x}.bin")
}

impl AcceptedModules {
    /// An in-memory set that never touches disk (tests, simulation).
    pub(crate) fn in_memory(stamp: Stamp) -> Self {
        Self {
            path: None,
            stamp,
            inner: Mutex::new(Inner {
                set: HashMap::new(),
                order: Vec::new(),
            }),
        }
    }

    /// Open the set stored in `dir` for `stamp`. Files for other engine identities
    /// in `dir` are deleted. Any problem leaves the set empty.
    pub(crate) fn open(dir: &Path, stamp: Stamp) -> Self {
        drop(std::fs::create_dir_all(dir));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            drop(std::fs::set_permissions(
                dir,
                std::fs::Permissions::from_mode(0o700),
            ));
        }
        let path = dir.join(file_name(stamp.engine_compat_id));
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".bin") && name != file_name(stamp.engine_compat_id) {
                    drop(std::fs::remove_file(e.path()));
                }
            }
        }
        let this = Self {
            path: Some(path.clone()),
            stamp,
            inner: Mutex::new(Inner {
                set: HashMap::new(),
                order: Vec::new(),
            }),
        };
        let loaded = Self::load(&path, &stamp);
        match loaded {
            Some((order, rewrite)) => {
                {
                    let mut inner = this.inner.lock().unwrap_or_else(|e| e.into_inner());
                    inner.set = order.iter().copied().collect();
                    inner.order = order.clone();
                }
                if rewrite {
                    this.rewrite(&order);
                }
            }
            None => {
                // Stale or damaged: start a fresh file with the current header.
                this.rewrite(&[]);
            }
        }
        this
    }

    /// The valid records and whether the file must be rewritten (earlier
    /// format, stray trailing bytes, repeated hashes or too many records).
    fn load(path: &Path, stamp: &Stamp) -> Option<(Vec<Record>, bool)> {
        let meta = std::fs::metadata(path).ok()?;
        if meta.len() > MAX_FILE_BYTES || meta.len() < HEADER_BYTES as u64 {
            return None;
        }
        let bytes = std::fs::read(path).ok()?;
        if bytes.len() < HEADER_BYTES {
            return None;
        }
        let legacy = bytes[..HEADER_BYTES] == header_v(stamp, LEGACY_VERSION)[..];
        if !legacy && bytes[..HEADER_BYTES] != header(stamp)[..] {
            return None;
        }
        let body = &bytes[HEADER_BYTES..];
        // A partial last record is ignored.
        let record_bytes = if legacy {
            LEGACY_RECORD_BYTES
        } else {
            RECORD_BYTES
        };
        let mut dirty = legacy || body.len() % record_bytes != 0;
        let mut order: Vec<Record> = if legacy {
            body.chunks_exact(LEGACY_RECORD_BYTES)
                .map(|c| (c.try_into().expect("32 bytes"), 0))
                .collect()
        } else {
            body.chunks_exact(RECORD_BYTES)
                .map(|c| {
                    (
                        c[..32].try_into().expect("32 bytes"),
                        u32::from_le_bytes(c[32..].try_into().expect("4 bytes")),
                    )
                })
                .collect()
        };
        // A repeated hash is not a valid record: keep the first.
        let mut seen = std::collections::HashSet::new();
        let before = order.len();
        order.retain(|(h, _)| seen.insert(*h));
        dirty |= order.len() != before;
        if order.len() > MAX_RECORDS {
            order.truncate(MAX_RECORDS);
            dirty = true;
        }
        Some((order, dirty))
    }

    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .set
            .len()
    }

    pub(crate) fn contains(&self, hash: &[u8; 32]) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .set
            .contains_key(hash)
    }

    /// The memory in MiB that compiling `hash` in this process needs, or `None`
    /// when it is not accepted. 0 means a record that carries no figure.
    pub(crate) fn requirement_mib(&self, hash: &[u8; 32]) -> Option<u32> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .set
            .get(hash)
            .copied()
    }

    /// Replace the file with `records` (temporary file, then rename).
    fn rewrite(&self, records: &[Record]) {
        let Some(path) = &self.path else { return };
        let tmp = path.with_extension(format!("tmp{}", std::process::id()));
        let result = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            let mut buf = header(&self.stamp);
            for (h, mib) in records {
                buf.extend_from_slice(h);
                buf.extend_from_slice(&mib.to_le_bytes());
            }
            f.write_all(&buf)?;
            f.sync_all()?;
            std::fs::rename(&tmp, path)
        })();
        if result.is_err() {
            drop(std::fs::remove_file(&tmp));
        }
    }

    /// Remember `hash`, whose compile needs `requirement_mib` MiB (0 when not
    /// known). Returns whether it was new.
    pub(crate) fn insert(&self, hash: [u8; 32], requirement_mib: u32) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.set.contains_key(&hash) {
            return false;
        }
        inner.set.insert(hash, requirement_mib);
        inner.order.push((hash, requirement_mib));
        if inner.order.len() > MAX_RECORDS {
            let cut = inner.order.len() - MAX_RECORDS / 2;
            let keep = inner.order.split_off(cut);
            inner.order = keep;
            inner.set = inner.order.iter().copied().collect();
            let snapshot = inner.order.clone();
            self.rewrite(&snapshot);
            return true;
        }
        if let Some(path) = &self.path {
            // One 36-byte append: atomic in practice on a local filesystem. If the
            // file was removed underneath us the append fails and the entry lives
            // in memory only.
            drop(
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(path)
                    .and_then(|mut f| {
                        let mut rec = [0u8; RECORD_BYTES];
                        rec[..32].copy_from_slice(&hash);
                        rec[32..].copy_from_slice(&requirement_mib.to_le_bytes());
                        f.write_all(&rec)
                    }),
            );
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(n: u8) -> Stamp {
        Stamp {
            engine_compat_id: 0xabc,
            caps_digest: [n; 16],
        }
    }
    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("fnam-{tag}-{}", std::process::id()));
        drop(std::fs::remove_dir_all(&d));
        d
    }
    fn h(n: u32) -> [u8; 32] {
        *blake3::hash(&n.to_le_bytes()).as_bytes()
    }

    #[test]
    fn survives_a_restart() {
        let d = dir("restart");
        let a = AcceptedModules::open(&d, stamp(1));
        assert!(a.insert(h(1), 0));
        assert!(!a.insert(h(1), 0));
        assert!(a.insert(h(2), 0));
        drop(a);
        let b = AcceptedModules::open(&d, stamp(1));
        assert_eq!(
            (b.len(), b.contains(&h(1)), b.contains(&h(2))),
            (2, true, true)
        );
        assert!(!b.contains(&h(3)));
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn a_changed_caps_version_or_engine_invalidates() {
        let d = dir("stamp");
        let a = AcceptedModules::open(&d, stamp(1));
        a.insert(h(1), 0);
        drop(a);
        // Different caps digest: the file is ignored and rewritten empty.
        let b = AcceptedModules::open(&d, stamp(2));
        assert_eq!(b.len(), 0);
        b.insert(h(9), 0);
        drop(b);
        let c = AcceptedModules::open(&d, stamp(2));
        assert_eq!(c.len(), 1);
        drop(c);
        // Different engine: another file name, the old file is deleted.
        let other = Stamp {
            engine_compat_id: 0xdef,
            caps_digest: [2; 16],
        };
        let e = AcceptedModules::open(&d, other);
        assert_eq!(e.len(), 0);
        let names: Vec<_> = std::fs::read_dir(&d)
            .unwrap()
            .map(|x| x.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec![file_name(0xdef)]);
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn corrupt_files_read_as_empty_and_are_replaced() {
        for (tag, content) in [
            ("garbage", b"not a header at all, just bytes".to_vec()),
            ("empty", Vec::new()),
            ("short", MAGIC.to_vec()),
        ] {
            let d = dir(tag);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join(file_name(0xabc)), content).unwrap();
            let a = AcceptedModules::open(&d, stamp(1));
            assert_eq!(a.len(), 0, "{tag}");
            a.insert(h(5), 0);
            drop(a);
            assert_eq!(AcceptedModules::open(&d, stamp(1)).len(), 1, "{tag}");
            std::fs::remove_dir_all(&d).unwrap();
        }
    }

    #[test]
    fn a_partial_trailing_record_is_ignored() {
        let d = dir("partial");
        let a = AcceptedModules::open(&d, stamp(1));
        a.insert(h(1), 0);
        a.insert(h(2), 0);
        let path = d.join(file_name(0xabc));
        drop(a);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&[7u8; 20]).unwrap();
        drop(f);
        let b = AcceptedModules::open(&d, stamp(1));
        assert_eq!((b.len(), b.contains(&h(2))), (2, true));
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn a_partial_trailing_record_does_not_misalign_later_appends() {
        let d = dir("misalign");
        let a = AcceptedModules::open(&d, stamp(1));
        a.insert(h(1), 5);
        let path = d.join(file_name(0xabc));
        drop(a);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&[7u8; 20]).unwrap();
        drop(f);
        let b = AcceptedModules::open(&d, stamp(1));
        assert!(b.insert(h(2), 9));
        drop(b);
        let c = AcceptedModules::open(&d, stamp(1));
        assert_eq!(c.len(), 2, "no corrupted record loaded");
        assert_eq!(
            (c.requirement_mib(&h(1)), c.requirement_mib(&h(2))),
            (Some(5), Some(9))
        );
        let len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(len as usize, HEADER_BYTES + 2 * RECORD_BYTES);
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn a_repeated_hash_is_dropped_at_load() {
        let d = dir("dup");
        let a = AcceptedModules::open(&d, stamp(1));
        a.insert(h(1), 5);
        let path = d.join(file_name(0xabc));
        drop(a);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        let mut rec = h(1).to_vec();
        rec.extend_from_slice(&99u32.to_le_bytes());
        f.write_all(&rec).unwrap();
        drop(f);
        let b = AcceptedModules::open(&d, stamp(1));
        assert_eq!((b.len(), b.requirement_mib(&h(1))), (1, Some(5)));
        assert_eq!(
            std::fs::metadata(&path).unwrap().len() as usize,
            HEADER_BYTES + RECORD_BYTES
        );
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn an_oversized_file_is_not_read() {
        let d = dir("big");
        std::fs::create_dir_all(&d).unwrap();
        let mut content = header(&stamp(1));
        content.resize(MAX_FILE_BYTES as usize + 1000, 1);
        std::fs::write(d.join(file_name(0xabc)), content).unwrap();
        let a = AcceptedModules::open(&d, stamp(1));
        assert_eq!(a.len(), 0);
        // And the replacement is small again.
        let len = std::fs::metadata(d.join(file_name(0xabc))).unwrap().len();
        assert_eq!(len, HEADER_BYTES as u64);
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn compaction_keeps_the_newest_half_and_the_file_stays_bounded() {
        let d = dir("compact");
        let a = AcceptedModules::open(&d, stamp(1));
        for i in 0..(MAX_RECORDS as u32 + 5) {
            a.insert(h(i), 0);
        }
        assert!(a.len() <= MAX_RECORDS);
        assert!(a.contains(&h(MAX_RECORDS as u32 + 4)), "newest kept");
        assert!(!a.contains(&h(0)), "oldest dropped");
        let len = std::fs::metadata(d.join(file_name(0xabc))).unwrap().len();
        assert!(len <= MAX_FILE_BYTES);
        drop(a);
        let b = AcceptedModules::open(&d, stamp(1));
        assert!(b.contains(&h(MAX_RECORDS as u32 + 4)));
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn concurrent_writers_lose_nothing() {
        let d = dir("conc");
        let a = std::sync::Arc::new(AcceptedModules::open(&d, stamp(1)));
        let hs: Vec<_> = (0..8u32)
            .map(|t| {
                let a = a.clone();
                std::thread::spawn(move || {
                    for i in 0..200u32 {
                        a.insert(h(t * 1000 + i), 0);
                    }
                })
            })
            .collect();
        for x in hs {
            x.join().unwrap();
        }
        assert_eq!(a.len(), 1600);
        drop(a);
        let b = AcceptedModules::open(&d, stamp(1));
        assert_eq!(b.len(), 1600, "every append reached the file whole");
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn two_handles_on_one_file_append_whole_records() {
        // Two nodes in one process (tests) or a restart overlap: appends from
        // separate handles must not interleave inside a record.
        let d = dir("two");
        let a = AcceptedModules::open(&d, stamp(1));
        let path = d.join(file_name(0xabc));
        let hs: Vec<_> = (0..4u32)
            .map(|t| {
                let path = path.clone();
                std::thread::spawn(move || {
                    for i in 0..200u32 {
                        let mut f = std::fs::OpenOptions::new()
                            .append(true)
                            .open(&path)
                            .unwrap();
                        let mut rec = h(t * 1000 + i).to_vec();
                        rec.extend_from_slice(&0u32.to_le_bytes());
                        f.write_all(&rec).unwrap();
                    }
                })
            })
            .collect();
        for x in hs {
            x.join().unwrap();
        }
        drop(a);
        let b = AcceptedModules::open(&d, stamp(1));
        assert_eq!(b.len(), 800);
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn in_memory_never_touches_disk() {
        let a = AcceptedModules::in_memory(stamp(1));
        assert!(a.insert(h(1), 0));
        assert!(a.contains(&h(1)));
    }

    #[test]
    fn the_requirement_is_kept_across_a_restart() {
        let d = dir("req");
        let a = AcceptedModules::open(&d, stamp(1));
        a.insert(h(1), 700);
        a.insert(h(2), 0);
        assert_eq!(a.requirement_mib(&h(1)), Some(700));
        assert_eq!(a.requirement_mib(&h(3)), None);
        drop(a);
        let b = AcceptedModules::open(&d, stamp(1));
        assert_eq!(b.requirement_mib(&h(1)), Some(700));
        assert_eq!(b.requirement_mib(&h(2)), Some(0));
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn a_record_of_the_earlier_format_has_no_requirement_and_is_upgraded() {
        let d = dir("legacy");
        std::fs::create_dir_all(&d).unwrap();
        let mut content = header_v(&stamp(1), LEGACY_VERSION);
        content.extend_from_slice(&h(1));
        content.extend_from_slice(&h(2));
        std::fs::write(d.join(file_name(0xabc)), content).unwrap();
        let a = AcceptedModules::open(&d, stamp(1));
        assert_eq!((a.len(), a.requirement_mib(&h(1))), (2, Some(0)));
        // A new record appended after the upgrade reads back whole.
        a.insert(h(3), 800);
        drop(a);
        let b = AcceptedModules::open(&d, stamp(1));
        assert_eq!(
            (b.len(), b.requirement_mib(&h(2)), b.requirement_mib(&h(3))),
            (3, Some(0), Some(800))
        );
        std::fs::remove_dir_all(&d).unwrap();
    }
}

//! Pins that keep native connections unreachable until the connect op turns
//! them on (`operations::connect::native::NATIVE_DIAL_READY`, itself pinned
//! false where it is declared).
//!
//! - Nothing outside `transport/quic/native` names `connect_native` or
//!   `accept_native` in production code.
//! - `NativeWaiters::register`, which is visible to the native module only,
//!   is called in production by `setup::accept_native` and nothing else, so
//!   the attempt router's native lookup is filled only by a native accept.
//!
//! Both read the crate's source from disk. A file's production code is what
//! comes before its first `#[cfg(test)]` module, with `//` comments removed
//! and whitespace stripped, so a call rustfmt splits over lines reads as one.
//! Each pin also requires the text it looks for to exist where it must, so a
//! renamed function or a moved file fails the pin rather than emptying it.

use std::path::{Path, PathBuf};

const NATIVE_DIR: &str = "src/transport/quic/native";

/// Every `.rs` file under `dir`.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("readable source dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// A file's code before its first test module, without `//` comments and
/// whitespace.
fn production(source: &str) -> String {
    let cut = [
        "#[cfg(test)]\nmod ",
        "#[cfg(test)]\npub(crate) mod ",
        "#[cfg(test)]\npub(super) mod ",
    ]
    .iter()
    .filter_map(|anchor| source.find(anchor))
    .min()
    .unwrap_or(source.len());
    source[..cut]
        .lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The production code of every file in the crate, by path, except this
/// file: it is all test code, and its needles would match themselves.
fn crate_production() -> Vec<(PathBuf, String)> {
    let mut files = Vec::new();
    rust_files(&crate_root().join("src"), &mut files);
    let this = crate_root().join(NATIVE_DIR).join("pins.rs");
    assert!(files.contains(&this), "the scan sees this file's path");
    files.retain(|path| *path != this);
    assert!(files.len() > 100, "the scan found the crate's files");
    files
        .into_iter()
        .map(|path| {
            let source = std::fs::read_to_string(&path).expect("readable source");
            (path, production(&source))
        })
        .collect()
}

/// Occurrences of the identifier `name` in `code`: not part of a longer
/// identifier (`connect_native_offered_zero` is a telemetry field, not a call).
fn ident_count(code: &str, name: &str) -> usize {
    let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    code.match_indices(name)
        .filter(|(i, _)| {
            let before = code[..*i].chars().next_back();
            let after = code[i + name.len()..].chars().next();
            !before.is_some_and(is_ident) && !after.is_some_and(is_ident)
        })
        .count()
}

/// The body of the top-level `fn` declared as `signature` (whitespace
/// stripped) in `code`, from the signature to its closing brace, by brace
/// counting. Panics if the signature is missing or appears twice.
fn fn_body<'a>(code: &'a str, signature: &str) -> &'a str {
    let starts: Vec<usize> = code.match_indices(signature).map(|(i, _)| i).collect();
    assert_eq!(starts.len(), 1, "`{signature}` is declared exactly once");
    let start = starts[0];
    let open = start + code[start..].find('{').expect("a body");
    let mut depth = 0usize;
    for (i, c) in code[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &code[start..open + i + 1];
                }
            }
            _ => {}
        }
    }
    panic!("`{signature}` has no closing brace");
}

/// Nothing outside the native module names the native entry points in
/// production code; both are defined inside it.
#[test]
fn the_native_entry_points_are_not_called_outside_the_native_module() {
    let native = crate_root().join(NATIVE_DIR);
    let mut defined = 0;
    for (path, code) in crate_production() {
        for name in ["connect_native", "accept_native"] {
            let found = ident_count(&code, name);
            if path.starts_with(&native) {
                defined += code.matches(&format!("pub(crate)asyncfn{name}<")).count();
                continue;
            }
            assert_eq!(
                found,
                0,
                "{} names `{name}` in production code; native connections stay \
                 unreachable until NATIVE_DIAL_READY",
                path.display()
            );
        }
    }
    assert_eq!(
        defined, 2,
        "both entry points are defined in the native module"
    );
}

/// `NativeWaiters::register` has one production call, inside
/// `accept_native`, and nothing else in the native module registers a
/// waiter.
#[test]
fn only_the_native_accept_registers_a_waiter() {
    let native = crate_root().join(NATIVE_DIR);
    let mut calls = Vec::new();
    for (path, code) in crate_production() {
        if !path.starts_with(&native) {
            continue;
        }
        let n = code.matches(".register(").count();
        if n > 0 {
            calls.push((path.clone(), n, code));
        }
    }
    assert_eq!(
        calls.len(),
        1,
        "one native file registers: {:?}",
        calls.iter().map(|c| &c.0).collect::<Vec<_>>()
    );
    let (path, n, code) = &calls[0];
    assert!(path.ends_with("setup.rs"), "{}", path.display());
    assert_eq!(*n, 1, "one call");
    let body = fn_body(code, "pub(crate)asyncfnaccept_native<");
    assert!(
        body.contains(".register(peer.addr)"),
        "the call is inside accept_native"
    );
}

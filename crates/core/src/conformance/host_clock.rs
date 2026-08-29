//! Whether a contract's WASM reads the host wall clock.
//!
//! `freenet_time::__frnt__time__utc_now` gives a module the *evaluating peer's*
//! clock. For a delegate that is unremarkable: a delegate holds private per-node
//! state, is never replicated, and has no merge laws to satisfy. For a contract
//! it is a hole in the foundation this module exists to check. `update_state` is
//! required to be a function of its inputs — that is the whole reason replicas
//! converge — and a merge that reads the clock is not a function of its inputs,
//! so the laws in [`super::property`] are not merely violated by such a contract,
//! they are not well-formed statements about it. Two peers eleven minutes apart
//! can produce different states from the same delta and neither is wrong.
//!
//! Contract access to the clock is therefore DEPRECATED, and in a future release
//! the call will TRAP: the contract still loads, but any actual call to the
//! clock fails that operation with a diagnosable error (issue #5465). Trapping
//! is per-call, so a contract that imports the symbol without reaching it keeps
//! working and needs no re-key. This module is the detector both halves of the
//! deprecation share: the node warns at contract load, and `fdev verify-merge`
//! reports it as a code-level diagnostic, so a developer-facing answer and a
//! node-facing answer cannot disagree — the same reason [`super::verify_case`]
//! is shared.
//!
//! # Deliberately import-level, not reachability-level
//!
//! This answers exactly one question: does the module IMPORT the clock. It does
//! not walk the call graph, so a module that imports the function and never
//! calls it is reported too. That superset is deliberate, and it is what makes
//! the phasing safe: the warning fires on any import, the later trap fires on
//! any actual call, and a call cannot happen without the import — so nothing
//! traps that was not warned about first. A false positive costs a log line and
//! a docs link, and the census behind #5465 found nearly every importer calls it
//! exactly once, so the superset is nearly tight in practice. Trapping needs no
//! reachability analysis at all, which is why no such function follows this one.
//!
//! # Failure direction
//!
//! A module that cannot be parsed reports `false`. That is right for a warning:
//! a malformed module is rejected downstream by the WASM runtime with a precise
//! error, and pre-empting it here with "your contract uses a deprecated host
//! function" would be a misleading diagnosis of a different problem — the same
//! reasoning as `contract::debug_sections`.
//!
//! The same direction is taken for an individual import entry that fails to
//! decode: [`imports_host_clock`] skips it and keeps reading the section, so an
//! undecodable entry positioned BEFORE the clock import discards the clock
//! import along with itself and the module reports `false`. Such a module does
//! not load either — wasmtime's validator rejects it — so the under-report is
//! never the last word on it.

/// The host-function namespace a contract imports the wall clock from.
pub const HOST_CLOCK_NAMESPACE: &str = "freenet_time";

/// The host function that returns the evaluating peer's wall clock.
pub const HOST_CLOCK_IMPORT: &str = "__frnt__time__utc_now";

/// Where a contract author is sent to read what to do instead.
pub const HOST_CLOCK_DEPRECATION_DOC: &str = "https://github.com/freenet/freenet-core/blob/main/docs/architecture/contracts/README.md#contracts-must-not-read-the-host-clock";

/// Does this contract WASM import [`HOST_CLOCK_IMPORT`] from
/// [`HOST_CLOCK_NAMESPACE`]?
///
/// See the module docs for why this is an import check rather than a
/// reachability check, and why an unparseable module answers `false`.
pub fn imports_host_clock(wasm: &[u8]) -> bool {
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        match payload {
            Ok(wasmparser::Payload::ImportSection(reader)) => {
                for import in reader.into_imports().flatten() {
                    if import.module == HOST_CLOCK_NAMESPACE && import.name == HOST_CLOCK_IMPORT {
                        return true;
                    }
                }
                // Stopping here makes the verdict independent of anything wrong
                // with the rest of the module: a truncated tail can neither add
                // an import nor remove one, and letting a late parse error void
                // an import already read would silence the warning on exactly
                // the modules least worth trusting.
                //
                // It does rest on a core module having at most ONE import
                // section — and note where that guarantee comes from, because
                // it is NOT from `Parser`. `wasmparser::Parser` will happily
                // hand back a second `ImportSection`, so a module carrying an
                // empty import section followed by a real one declaring the
                // clock is parsed without complaint and answered `false` here.
                // The guarantee is wasmtime's VALIDATOR, which rejects a
                // duplicate section, and the call site is what makes that
                // load-bearing: both callers validate right afterwards
                // (`warn_on_host_clock_import` runs immediately before
                // `engine.compile()`; `fdev`'s `code_diagnostics` runs
                // immediately before the oracle instantiates the module), so
                // any module this short-circuit under-reports is a module that
                // then fails to load anyway. A caller that ever runs this
                // WITHOUT a validation step behind it does not inherit that,
                // and must scan every import section.
                return false;
            }
            // Not a section that can declare an import.
            Ok(_) => {}
            // See the module docs on failure direction.
            Err(_) => return false,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assemble a module importing one function per `(namespace, name)` pair.
    fn module_importing(imports: &[(&str, &str)]) -> Vec<u8> {
        let mut wat = String::from("(module\n");
        for (i, (namespace, name)) in imports.iter().enumerate() {
            wat.push_str(&format!(
                "  (import \"{namespace}\" \"{name}\" (func $f{i} (param i64 i64)))\n"
            ));
        }
        wat.push_str(")\n");
        wat::parse_str(&wat).expect("test fixture is valid wat")
    }

    #[test]
    fn a_module_importing_the_clock_is_detected() {
        let wasm = module_importing(&[(HOST_CLOCK_NAMESPACE, HOST_CLOCK_IMPORT)]);
        assert!(imports_host_clock(&wasm));
    }

    /// The clock is one import among many, which is the shape every deployed
    /// importer measured for #5465 actually has: a contract also logs, and the
    /// log import sorts before the time import.
    #[test]
    fn the_clock_is_found_among_other_imports() {
        let wasm = module_importing(&[
            ("freenet_log", "__frnt__logger__info"),
            (HOST_CLOCK_NAMESPACE, HOST_CLOCK_IMPORT),
            ("freenet_rand", "__frnt__rand__rand_bytes"),
        ]);
        assert!(imports_host_clock(&wasm));
    }

    #[test]
    fn a_module_importing_other_host_functions_is_not_flagged() {
        let wasm = module_importing(&[
            ("freenet_log", "__frnt__logger__info"),
            ("freenet_rand", "__frnt__rand__rand_bytes"),
        ]);
        assert!(!imports_host_clock(&wasm));
    }

    /// Both halves of the pair must match. A module importing some OTHER
    /// function from `freenet_time`, or `__frnt__time__utc_now` from some other
    /// namespace, is not importing the host clock — and a check that matched on
    /// the namespace alone, or on the function name alone, would pass every
    /// other test in this module while flagging contracts that read no clock.
    #[test]
    fn namespace_and_function_must_both_match() {
        let other_fn = module_importing(&[(HOST_CLOCK_NAMESPACE, "__frnt__time__something_else")]);
        assert!(!imports_host_clock(&other_fn));
        let other_ns = module_importing(&[("some_other_namespace", HOST_CLOCK_IMPORT)]);
        assert!(!imports_host_clock(&other_ns));
    }

    #[test]
    fn a_module_with_no_imports_at_all_is_not_flagged() {
        let wasm = module_importing(&[]);
        assert!(!imports_host_clock(&wasm));
    }

    /// The name appearing in the BYTES is not the same as the name appearing in
    /// the IMPORT SECTION. A substring search over the module — the obvious
    /// shortcut, and one that needs no wasm parser at all — reports `true` here,
    /// because a contract that merely mentions the string (in an export, a data
    /// segment, a panic message, or a `&str` constant) is indistinguishable from
    /// one that imports it.
    #[test]
    fn the_name_merely_appearing_in_the_module_is_not_an_import() {
        let wat = format!(
            r#"(module
                 (memory (export "memory") 1)
                 (data (i32.const 0) "{HOST_CLOCK_NAMESPACE}")
                 (data (i32.const 64) "{HOST_CLOCK_IMPORT}")
                 (func (export "{HOST_CLOCK_IMPORT}") (param i64 i64)))"#
        );
        let wasm = wat::parse_str(&wat).expect("test fixture is valid wat");
        assert!(
            wasm.windows(HOST_CLOCK_IMPORT.len())
                .any(|w| w == HOST_CLOCK_IMPORT.as_bytes()),
            "fixture must actually contain the name, or this test proves nothing"
        );
        assert!(!imports_host_clock(&wasm));
    }

    /// See "Failure direction" in the module docs.
    #[test]
    fn an_unparseable_module_is_not_flagged() {
        assert!(!imports_host_clock(b""));
        assert!(!imports_host_clock(b"not wasm at all"));
        // A truncated real module: the header parses, the rest does not.
        let mut truncated = module_importing(&[(HOST_CLOCK_NAMESPACE, HOST_CLOCK_IMPORT)]);
        truncated.truncate(10);
        assert!(!imports_host_clock(&truncated));
    }

    /// A parse error AFTER the import section must not void an import already
    /// read. `an_unparseable_module_is_not_flagged` truncates to 10 bytes and
    /// never reaches an import section, so it does not distinguish this.
    ///
    /// Mutation this exists for: collect the imports and decide after the parse
    /// loop finishes, instead of answering as soon as the pair is found. The
    /// trailing garbage then produces `Err` and the module reports clean —
    /// silencing the warning on exactly the modules least worth trusting.
    #[test]
    fn a_corrupt_tail_does_not_void_an_import_already_read() {
        let mut wasm = module_importing(&[(HOST_CLOCK_NAMESPACE, HOST_CLOCK_IMPORT)]);
        // 0xFF is not a valid section id, so the parser fails here and not before.
        wasm.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(
            imports_host_clock(&wasm),
            "a clock import already read was discarded by a later parse error"
        );
    }

    /// The known limit of the single-import-section short-circuit, pinned so it
    /// stays a documented decision rather than becoming an accident.
    ///
    /// `wasmparser::Parser` does not enforce one import section per module, so a
    /// module whose FIRST import section lacks the clock and whose second
    /// declares it is answered `false` here. That is safe only because
    /// wasmtime's validator refuses such a module downstream — see the comment
    /// at the short-circuit. If this test ever starts failing because the
    /// answer became `true`, that is an improvement, not a regression: delete
    /// the test and the caveat in the comment together.
    #[test]
    fn a_second_import_section_is_not_scanned_and_the_validator_is_why_that_is_safe() {
        let first = module_importing(&[("freenet_log", "__frnt__logger__info")]);
        let clock = module_importing(&[(HOST_CLOCK_NAMESPACE, HOST_CLOCK_IMPORT)]);
        // Splice `clock`'s import section (id 0x02) onto the end of `first`.
        let section = import_section_of(&clock);
        let mut two_sections = first.clone();
        two_sections.extend_from_slice(&section);
        assert_ne!(
            two_sections, first,
            "the fixture must actually add a section"
        );
        assert!(
            !imports_host_clock(&two_sections),
            "the short-circuit now scans past the first import section; if that \
             is deliberate, remove this test and the caveat it pins"
        );
        // ... and the module the short-circuit under-reports does not load.
        assert!(
            wasmparser::validate(&two_sections).is_err(),
            "a module with two import sections is now accepted by the validator, \
             so the short-circuit's safety argument no longer holds"
        );
    }

    /// The bytes of `wasm`'s import section, id byte and length prefix included.
    fn import_section_of(wasm: &[u8]) -> Vec<u8> {
        for payload in wasmparser::Parser::new(0).parse_all(wasm) {
            if let Ok(wasmparser::Payload::ImportSection(reader)) = payload {
                let range = reader.range();
                let range = (range.start as usize)..(range.end as usize);
                let mut out = vec![0x02];
                let mut len = (range.end - range.start) as u32;
                loop {
                    let byte = (len & 0x7f) as u8;
                    len >>= 7;
                    if len == 0 {
                        out.push(byte);
                        break;
                    }
                    out.push(byte | 0x80);
                }
                out.extend_from_slice(&wasm[range]);
                return out;
            }
        }
        panic!("fixture has no import section");
    }

    /// The docs link the node's WARN and every `fdev` diagnostic point at must
    /// resolve to a heading that exists.
    ///
    /// Mutation this exists for: reword the "Contracts must not read the host
    /// clock" heading. Nothing else in this feature notices, and every operator
    /// warned about a deprecated capability lands on a page with no such
    /// section.
    #[test]
    fn the_deprecation_doc_link_points_at_a_heading_that_exists() {
        const DOC: &str = include_str!("../../../../docs/architecture/contracts/README.md");
        const DOC_PATH: &str = "docs/architecture/contracts/README.md";

        assert!(
            HOST_CLOCK_DEPRECATION_DOC.contains(DOC_PATH),
            "the deprecation link no longer points at {DOC_PATH}, so this test is \
             reading a different file from the one operators are sent to: {HOST_CLOCK_DEPRECATION_DOC}"
        );
        let (_, fragment) = HOST_CLOCK_DEPRECATION_DOC
            .split_once('#')
            .expect("the deprecation link carries no heading anchor");

        /// GitHub's heading-anchor rule, near enough: lowercase, drop
        /// punctuation, spaces to hyphens.
        fn anchor(heading: &str) -> String {
            heading
                .trim_start_matches('#')
                .trim()
                .to_lowercase()
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-')
                .collect::<String>()
                .replace(' ', "-")
        }

        let matches = DOC
            .lines()
            .filter(|line| line.starts_with('#'))
            .filter(|line| anchor(line) == fragment)
            .count();
        assert_eq!(
            matches, 1,
            "the anchor `#{fragment}` matches {matches} headings in {DOC_PATH}; the \
             node's deprecation warning and every fdev diagnostic link there"
        );
    }
}

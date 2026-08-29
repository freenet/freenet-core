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
//! Contract access to the clock is therefore DEPRECATED and will be refused in a
//! future release (issue #5465). This module is the detector both halves of the
//! deprecation share: the node warns at contract load, and `fdev verify-merge`
//! reports it as a code-level diagnostic, so a developer-facing answer and a
//! node-facing answer cannot disagree — the same reason [`super::verify_case`]
//! is shared.
//!
//! # Deliberately import-level, not reachability-level
//!
//! This answers exactly one question: does the module IMPORT the clock. It does
//! not walk the call graph from the state-producing entry points, so a module
//! that imports the function and never calls it is reported too. That is a
//! deliberate superset for the WARNING stage — a false positive costs a log line
//! and a docs link, and the census behind #5465 found 32 of 33 importers call it
//! exactly once, so the superset is nearly tight in practice. The later stage
//! that REFUSES to load such a contract is the one that needs reachability, and
//! it is not this function.
//!
//! # Failure direction
//!
//! A module that cannot be parsed reports `false`. That is right for a warning:
//! a malformed module is rejected downstream by the WASM runtime with a precise
//! error, and pre-empting it here with "your contract uses a deprecated host
//! function" would be a misleading diagnosis of a different problem — the same
//! reasoning as `contract::debug_sections`. A future refusal check must fail the
//! OTHER way (refuse what it cannot parse), which is one more reason it is a
//! separate function rather than a stricter mode of this one.

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
                // A core module has at most ONE import section and it precedes
                // every section that could carry a call, so the answer is final
                // here. Stopping also makes the verdict independent of anything
                // wrong with the rest of the module: a truncated tail can
                // neither add an import nor remove one, and letting a late parse
                // error void an import we have already read would silence the
                // warning on exactly the modules least worth trusting.
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

    /// The clock is one import among many, which is the shape every one of the
    /// 33 deployed importers actually has: a contract also logs, and the log
    /// import sorts before the time import.
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
}

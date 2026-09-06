//! The #5465 deprecation warning actually reaches an operator's log.
//!
//! Everything else about `warn_on_host_clock_import` is unit-tested on its
//! return value, which is cheap and pins the once-per-contract decision — but a
//! `bool` cannot tell `tracing::warn!` from `tracing::debug!`. Changing that one
//! token compiles clean, passes `clippy -D warnings`, leaves every unit test and
//! both source-scrape pins green, and silences the entire operator-facing half
//! of this feature at default log levels. This module is the test that goes red
//! for it.
//!
//! It is deliberately built on the real thing at every layer that could rot:
//!
//! - the modules are `tests/test-contract-2` and `tests/test-contract-1`,
//!   compiled by `rustc` against a published `freenet-stdlib`, so the import
//!   strings come from the ABI as third-party contract authors actually meet it
//!   — not from `HOST_CLOCK_NAMESPACE` / `HOST_CLOCK_IMPORT`, which is what
//!   every other test in this feature asserts against on BOTH sides of its own
//!   assertion;
//! - the call is `Runtime::prepare_contract_call`, the production entry point,
//!   rather than `warn_on_host_clock_import` directly, so deleting the call site
//!   fails here and not only in the source-scrape pin;
//! - the assertion anchors on `HOST_CLOCK_DEPRECATION_DOC`, so rewording the
//!   message does not fail it.
//!
//! The whole module is gated on `trace` by its declaration in `tests.rs`, since
//! `tracing_subscriber` is only a dependency under that feature. It is on by
//! default and CI's test job enables it explicitly, so these run.

use std::sync::Arc;

use freenet_stdlib::prelude::{
    ContractCode, ContractContainer, ContractKey, ContractWasmAPIVersion, WrappedContract,
};

use super::super::Runtime;
use crate::contract::storages::Storage;
use crate::util::tests::get_temp_dir;

/// Build a runtime holding `module`, with `tag` appended as a WASM custom
/// section, and return it with the contract's key.
///
/// The tag is not cosmetic. `SEEN` in `warn_on_host_clock_import` is
/// process-global and dedups per code hash, and `tests::time::now` loads
/// `test-contract-2` in this same test binary. Sharing its bytes would make the
/// test below pass or fail on which one `cargo test` happened to schedule first
/// — precisely the process-global cross-test coupling `.claude/rules/testing.md`
/// exists about, and invisible under `cargo nextest`, which CI runs. A custom
/// section leaves the import section untouched (so the module still genuinely
/// imports whatever it imported) while giving each test a code hash no other
/// test can consume.
async fn runtime_holding(
    module: &str,
    tag: &str,
) -> Result<(tempfile::TempDir, Runtime, ContractKey, Vec<u8>), Box<dyn std::error::Error>> {
    let temp_dir = get_temp_dir();
    let db = Storage::new(temp_dir.path()).await?;
    let mut contract_store =
        super::super::ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())?;
    let delegate_store =
        super::super::DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())?;
    let secrets_store =
        super::super::SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db)?;

    let code = super::code_variant(&super::get_test_module(module)?, tag);
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(code.clone())),
        vec![].into(),
    )));
    let key = contract.key();
    contract_store.store_contract(contract)?;

    let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)?;
    Ok((temp_dir, runtime, key, code))
}

/// Loading a contract that reads the host clock emits a WARN naming the docs.
///
/// Mutation this exists for: `tracing::warn!` -> `tracing::debug!` in
/// `warn_on_host_clock_import`. Also red if the call site is deleted, if the
/// detector stops matching a real stdlib-built module, or if the names the
/// linker registers drift from the constants the detector matches on.
#[tokio::test(flavor = "current_thread")]
async fn a_clock_reading_contract_warns_at_load() -> Result<(), Box<dyn std::error::Error>> {
    let (temp_dir, mut runtime, key, code) =
        runtime_holding("test_contract_2", "host_clock_warns_at_load").await?;

    // Self-check: the point of this fixture is that a REAL stdlib build imports
    // the clock under the names the detector matches. If that stopped being
    // true the assertion below would fail for an uninformative reason, so say
    // which half broke.
    assert!(
        crate::conformance::imports_host_clock(&code),
        "tests/test-contract-2 no longer imports the host clock under the names \
         the detector matches, so this test cannot exercise the warning"
    );

    // Installed after every await, so the capture and the call it captures are
    // on one thread.
    let (messages, guard) = crate::util::test_log_capture::install();
    runtime.prepare_contract_call(&key, &vec![].into(), 1_000)?;
    drop(guard);

    let logs = messages.lock().unwrap();
    let warnings: Vec<&String> = logs
        .iter()
        .filter(|line| {
            line.starts_with("WARN")
                && line.contains(crate::conformance::HOST_CLOCK_DEPRECATION_DOC)
        })
        .collect();
    assert_eq!(
        warnings.len(),
        1,
        "loading a clock-reading contract must emit exactly one WARN carrying \
         the deprecation docs link; captured:\n{logs:#?}"
    );
    // The operator has to be able to tell WHICH contract to fix.
    assert!(
        warnings[0].contains(&key.to_string()),
        "the deprecation warning does not name the contract:\n{}",
        warnings[0]
    );
    drop(logs);

    drop(temp_dir);
    Ok(())
}

/// A contract that does not read the clock draws no warning.
///
/// Without this, "warn unconditionally" would satisfy the test above.
#[tokio::test(flavor = "current_thread")]
async fn a_contract_that_reads_no_clock_is_silent_at_load() -> Result<(), Box<dyn std::error::Error>>
{
    let (temp_dir, mut runtime, key, code) =
        runtime_holding("test_contract_1", "host_clock_silent_at_load").await?;

    assert!(
        !crate::conformance::imports_host_clock(&code),
        "tests/test-contract-1 has started importing the host clock, so it can \
         no longer serve as the negative control here"
    );

    let (messages, guard) = crate::util::test_log_capture::install();
    runtime.prepare_contract_call(&key, &vec![].into(), 1_000)?;
    drop(guard);

    let logs = messages.lock().unwrap();
    assert!(
        !logs
            .iter()
            .any(|line| line.contains(crate::conformance::HOST_CLOCK_DEPRECATION_DOC)),
        "a contract that never reads the clock was warned about anyway:\n{logs:#?}"
    );
    drop(logs);

    drop(temp_dir);
    Ok(())
}

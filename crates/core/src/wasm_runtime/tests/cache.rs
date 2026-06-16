//! Tests for WASM module cache behavior (byte-budget LRU eviction).
//!
//! These exercise the real `Runtime` against bundled compiled contracts. The
//! pure eviction-by-bytes logic is unit-tested without WASM in
//! `wasm_runtime::module_cache::tests`; here we confirm the real cache tracks
//! real compiled-module sizes and stays within its byte budget.

use std::sync::Arc;

use super::super::Runtime;
use super::super::contract::ContractRuntimeInterface;
use super::super::runtime::RuntimeConfig;
use super::{TestSetup, get_test_module, setup_test_contract};
use freenet_stdlib::prelude::*;

/// Loading a contract populates the cache and the cache stays within its byte
/// budget; loading the same contract again is a hit (no second entry).
#[tokio::test(flavor = "multi_thread")]
async fn test_module_cache_tracks_bytes_and_hits() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key: contract_key_1,
        temp_dir,
    } = setup_test_contract("test_contract_1").await?;

    // A generous budget — one module easily fits.
    let config = RuntimeConfig {
        module_cache_budget_bytes: 64 * 1024 * 1024,
        ..Default::default()
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let state = WrappedState::new(vec![]);
    let _result = runtime.validate_state(
        &contract_key_1,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );
    {
        let cache = runtime.contract_modules.lock().unwrap();
        assert_eq!(cache.len(), 1, "Cache should have 1 entry after first load");
        assert!(
            cache.total_bytes() > 0,
            "Cache should track a non-zero compiled size for the loaded module"
        );
        assert!(
            cache.total_bytes() <= cache.budget_bytes(),
            "tracked bytes {} must stay within budget {}",
            cache.total_bytes(),
            cache.budget_bytes()
        );
    }

    // Load the same contract again — cache hit, still 1 entry.
    let _result = runtime.validate_state(
        &contract_key_1,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );
    assert_eq!(
        runtime.contract_modules.lock().unwrap().len(),
        1,
        "Cache should still have 1 entry (cache hit)"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// A budget smaller than a single compiled module still loads the contract
/// (the oversized entry is retained so the contract can run) and the cache
/// reports the real compiled size, exceeding the (tiny) budget by design for
/// the single resident entry.
#[tokio::test(flavor = "multi_thread")]
async fn test_module_cache_tiny_budget_still_runs() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = setup_test_contract("test_contract_1").await?;

    // 1-byte budget: clamps to keeping exactly one resident entry.
    let config = RuntimeConfig {
        module_cache_budget_bytes: 1,
        ..Default::default()
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    // A 4-byte [1,2,3,4] state makes test-contract-1 return Valid, proving the
    // module both compiled and executed under the tiny budget.
    let state = WrappedState::new(vec![1, 2, 3, 4]);
    let result = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );
    assert!(
        matches!(result, Ok(ValidateResult::Valid)),
        "contract must still compile+execute under a tiny budget: {result:?}"
    );

    let cache = runtime.contract_modules.lock().unwrap();
    assert_eq!(cache.len(), 1, "single oversized entry retained");
    assert!(
        cache.total_bytes() > cache.budget_bytes(),
        "the lone resident module legitimately exceeds the 1-byte budget"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// Build `count` distinct `ContractKey`s from the SAME WASM code by varying the
/// parameters, store/index each in the contract store, and return the keys.
///
/// Each key is a separate cache entry (cache is keyed by the full
/// `ContractKey`), so loading all of them exercises eviction with real compiled
/// module sizes without needing many distinct contract source files.
fn distinct_keys_same_code(
    contract_store: &mut super::super::ContractStore,
    code: &[u8],
    count: usize,
) -> Vec<ContractKey> {
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let params = Parameters::from(format!("param-{i}").into_bytes());
        let wrapped = WrappedContract::new(
            Arc::new(ContractCode::from(code.to_vec())),
            params.clone().into_owned(),
        );
        let key = *wrapped.key();
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped));
        contract_store
            .store_contract(container)
            .expect("store distinct-param contract");
        contract_store
            .ensure_key_indexed(&key)
            .expect("index distinct-param key");
        keys.push(key);
    }
    keys
}

/// REGRESSION (issue #4441): the cache evicts by BYTES, not by a fixed entry
/// count. Loading many distinct modules under a byte budget that only holds a
/// few must keep `total_bytes <= budget` and evict the rest — and crucially the
/// count of resident entries is far BELOW the old 1024-entry count cap, proving
/// eviction is driven by size, not count.
///
/// On `origin/main` (count-capped `LruCache` with capacity 1024) this test
/// fails: all the loaded entries stay resident (well under 1024), the byte
/// total grows unbounded relative to any byte budget, and there is no
/// `total_bytes`/`budget_bytes` accounting at all (the methods don't exist).
#[tokio::test(flavor = "multi_thread")]
async fn test_module_cache_evicts_by_bytes_not_count() -> Result<(), Box<dyn std::error::Error>> {
    let code = get_test_module("test_contract_1")?;

    let TestSetup {
        mut contract_store,
        delegate_store,
        secrets_store,
        temp_dir,
        ..
    } = setup_test_contract("test_contract_1").await?;

    // Create 8 distinct-param keys over the same code.
    let keys = distinct_keys_same_code(&mut contract_store, &code, 8);

    // First, measure one compiled module's size with a generous budget so we
    // can pick a byte budget that holds only ~2 modules.
    let probe_config = RuntimeConfig {
        module_cache_budget_bytes: 512 * 1024 * 1024,
        ..Default::default()
    };
    let mut probe_runtime = Runtime::build_with_config(
        contract_store,
        delegate_store,
        secrets_store,
        false,
        probe_config,
    )
    .unwrap();
    let valid_state = WrappedState::new(vec![1, 2, 3, 4]);
    let params0 = Parameters::from("param-0".as_bytes().to_vec());
    // We only care that the module compiled and got cached; the validate
    // outcome is irrelevant here, so explicitly discard the must-use result.
    drop(probe_runtime.validate_state(&keys[0], &params0, &valid_state, &Default::default()));
    let per_module = {
        let cache = probe_runtime.contract_modules.lock().unwrap();
        assert_eq!(cache.len(), 1);
        cache.total_bytes()
    };
    assert!(per_module > 0, "compiled module size must be measurable");

    // Budget that holds at most 2 such modules.
    let budget = per_module * 2 + per_module / 2; // between 2x and 3x
    {
        let mut cache = probe_runtime.contract_modules.lock().unwrap();
        *cache = super::super::ModuleCache::new(budget);
    }

    // Load all 8 distinct modules.
    for (i, key) in keys.iter().enumerate() {
        let params = Parameters::from(format!("param-{i}").into_bytes());
        drop(probe_runtime.validate_state(key, &params, &valid_state, &Default::default()));

        let cache = probe_runtime.contract_modules.lock().unwrap();
        assert!(
            cache.total_bytes() <= cache.budget_bytes(),
            "after loading {} modules, total_bytes {} exceeded budget {}",
            i + 1,
            cache.total_bytes(),
            cache.budget_bytes()
        );
    }

    let cache = probe_runtime.contract_modules.lock().unwrap();
    // The budget holds ~2 modules, so far fewer than 8 are resident — and
    // certainly far below the old 1024 count cap.
    assert!(
        cache.len() <= 3,
        "byte budget should keep ~2-3 modules resident, got {}",
        cache.len()
    );
    assert!(
        cache.len() < 8,
        "eviction must have dropped some of the 8 loaded modules"
    );
    assert!(
        cache.total_bytes() <= cache.budget_bytes(),
        "final total_bytes {} must be within budget {}",
        cache.total_bytes(),
        cache.budget_bytes()
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// Measure and log the real compiled size of a representative contract module,
/// documenting the basis for `DEFAULT_MODULE_CACHE_BUDGET_BYTES`. Asserts the
/// measured size is in a sane range (10 KiB .. 16 MiB) so the default budget's
/// "holds hundreds of modules" claim stays grounded if the toolchain changes.
#[tokio::test(flavor = "multi_thread")]
async fn test_compiled_module_size_is_in_expected_range() -> Result<(), Box<dyn std::error::Error>>
{
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = setup_test_contract("test_contract_1").await?;

    let mut runtime = Runtime::build_with_config(
        contract_store,
        delegate_store,
        secrets_store,
        false,
        RuntimeConfig::default(),
    )
    .unwrap();

    let state = WrappedState::new(vec![1, 2, 3, 4]);
    drop(runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    ));

    let measured = runtime.contract_modules.lock().unwrap().total_bytes();
    println!(
        "MEASURED compiled module size for test_contract_1: {measured} bytes \
         ({:.2} MiB); DEFAULT_MODULE_CACHE_BUDGET_BYTES = {} bytes ({} MiB) \
         holds ~{} such modules",
        measured as f64 / (1024.0 * 1024.0),
        super::super::DEFAULT_MODULE_CACHE_BUDGET_BYTES,
        super::super::DEFAULT_MODULE_CACHE_BUDGET_BYTES / (1024 * 1024),
        if measured > 0 {
            super::super::DEFAULT_MODULE_CACHE_BUDGET_BYTES / measured
        } else {
            0
        },
    );

    assert!(
        (10 * 1024..=16 * 1024 * 1024).contains(&measured),
        "compiled module size {measured} bytes outside expected 10KiB..16MiB range; \
         revisit DEFAULT_MODULE_CACHE_BUDGET_BYTES if the toolchain changed"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

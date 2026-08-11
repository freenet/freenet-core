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

/// Store `code` under `params` and return the resulting key, indexed so the
/// runtime can resolve it.
fn store_and_index(
    contract_store: &mut super::super::ContractStore,
    code: &[u8],
    params: Parameters<'static>,
) -> ContractKey {
    let wrapped = WrappedContract::new(Arc::new(ContractCode::from(code.to_vec())), params);
    let key = *wrapped.key();
    let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped));
    contract_store
        .store_contract(container)
        .expect("store test contract");
    contract_store
        .ensure_key_indexed(&key)
        .expect("index test contract key");
    key
}

/// Build `count` distinct `ContractKey`s from the SAME WASM code by varying the
/// parameters, store/index each in the contract store, and return the keys.
///
/// Since `instance = blake3(code_hash ‖ params)`, every one of these is a
/// distinct contract as far as routing and storage are concerned — but they all
/// compile to the same module, so the module cache must hold exactly ONE entry
/// for the whole set (#5268).
fn distinct_keys_same_code(
    contract_store: &mut super::super::ContractStore,
    code: &[u8],
    count: usize,
) -> Vec<ContractKey> {
    (0..count)
        .map(|i| {
            let params = Parameters::from(format!("param-{i}").into_bytes());
            store_and_index(contract_store, code, params)
        })
        .collect()
}

/// LEB128-encode `value` (unsigned) onto `out` — the length encoding WASM
/// sections use.
fn push_leb128_u32(mut value: u32, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

/// Produce a variant of `code` that is a DIFFERENT binary (different bytes,
/// therefore a different code hash) but behaves identically, by appending a WASM
/// custom section tagged with `tag`.
///
/// Custom sections carry no semantics — the runtime ignores them — so this gives
/// the tests several genuinely distinct modules without needing several distinct
/// contract source crates (each of which costs a `cargo build` in `get_test_module`).
fn code_variant(code: &[u8], tag: u32) -> Vec<u8> {
    const SECTION_NAME: &str = "freenet-module-cache-test-variant";
    let mut payload = Vec::new();
    push_leb128_u32(SECTION_NAME.len() as u32, &mut payload);
    payload.extend_from_slice(SECTION_NAME.as_bytes());
    payload.extend_from_slice(&tag.to_le_bytes());

    let mut out = code.to_vec();
    out.push(0); // custom section id
    push_leb128_u32(payload.len() as u32, &mut out);
    out.extend_from_slice(&payload);
    out
}

/// Build `count` contracts over `count` DISTINCT binaries (same behavior,
/// different bytes), all under the same parameters.
fn distinct_keys_distinct_code(
    contract_store: &mut super::super::ContractStore,
    code: &[u8],
    count: usize,
) -> Vec<ContractKey> {
    (0..count)
        .map(|i| {
            let variant = code_variant(code, i as u32);
            store_and_index(
                contract_store,
                &variant,
                Parameters::from([].as_ref()).into_owned(),
            )
        })
        .collect()
}

/// REGRESSION (#5268): contracts that share WASM code but differ in parameters
/// must share ONE compiled module.
///
/// Compilation never sees parameters — `prepare_contract_call_inner` compiles
/// `contract_v1.code().data()` and nothing else — but `ContractKey`'s `Hash`/`Eq`
/// compare only `instance = blake3(code_hash ‖ params)`. Keying the module cache
/// by `ContractKey` therefore compiled and retained one copy of the same machine
/// code per parameter set: 3,746 resident modules for 215 distinct `.wasm` files
/// on a measured gateway, ~247 MiB of the cache wasted, and enough eviction
/// pressure to keep 61% of fleet samples pinned at ≥99% of the budget.
///
/// Without the fix this asserts 1 == 4 and fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_same_code_different_params_share_one_module() -> Result<(), Box<dyn std::error::Error>>
{
    let code = get_test_module("test_contract_1")?;
    let TestSetup {
        mut contract_store,
        delegate_store,
        secrets_store,
        temp_dir,
        ..
    } = setup_test_contract("test_contract_1").await?;

    let keys = distinct_keys_same_code(&mut contract_store, &code, 4);

    // Generous budget: nothing is evicted, so the resident count is purely a
    // question of how many entries the keying produces.
    let config = RuntimeConfig {
        module_cache_budget_bytes: 512 * 1024 * 1024,
        ..Default::default()
    };
    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let state = WrappedState::new(vec![1, 2, 3, 4]);
    for (i, key) in keys.iter().enumerate() {
        let params = Parameters::from(format!("param-{i}").into_bytes());
        let result = runtime.validate_state(key, &params, &state, &Default::default());
        assert!(
            matches!(result, Ok(ValidateResult::Valid)),
            "contract {i} must execute: {result:?}"
        );
    }

    let cache = runtime.contract_modules.lock().unwrap();
    assert_eq!(
        cache.len(),
        1,
        "{} contracts over one binary must share ONE compiled module, got {} entries",
        keys.len(),
        cache.len()
    );

    std::mem::drop(cache);
    std::mem::drop(temp_dir);
    Ok(())
}

/// The DANGEROUS direction of #5268: contracts with DIFFERENT code must never
/// share a cache entry. Deduplicating too aggressively would run one contract's
/// compiled module on another's behalf, which is far worse than the memory waste
/// the dedup exists to fix.
#[tokio::test(flavor = "multi_thread")]
async fn test_distinct_code_never_shares_a_module() -> Result<(), Box<dyn std::error::Error>> {
    let code = get_test_module("test_contract_1")?;
    let TestSetup {
        mut contract_store,
        delegate_store,
        secrets_store,
        temp_dir,
        ..
    } = setup_test_contract("test_contract_1").await?;

    let keys = distinct_keys_distinct_code(&mut contract_store, &code, 3);
    // Distinct binaries ⇒ distinct code hashes. (If `code_variant` ever stopped
    // changing the bytes, the rest of this test would silently pass for the
    // wrong reason.)
    let hashes: std::collections::HashSet<_> = keys.iter().map(|k| *k.code_hash()).collect();
    assert_eq!(
        hashes.len(),
        keys.len(),
        "variants must differ in code hash"
    );

    let config = RuntimeConfig {
        module_cache_budget_bytes: 512 * 1024 * 1024,
        ..Default::default()
    };
    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let state = WrappedState::new(vec![1, 2, 3, 4]);
    for (i, key) in keys.iter().enumerate() {
        let result = runtime.validate_state(
            key,
            &Parameters::from([].as_ref()),
            &state,
            &Default::default(),
        );
        assert!(
            matches!(result, Ok(ValidateResult::Valid)),
            "variant {i} must compile and execute on its own module: {result:?}"
        );
        assert_eq!(
            runtime.contract_modules.lock().unwrap().len(),
            i + 1,
            "each distinct binary must add its own cache entry"
        );
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// REGRESSION (issue #4441): the cache evicts by BYTES, not by a fixed entry
/// count. Loading many distinct modules under a byte budget that only holds a
/// few must keep `total_bytes <= budget` and evict the rest — and crucially the
/// count of resident entries is far BELOW the old 1024-entry count cap, proving
/// eviction is driven by size, not count.
///
/// Forward regression test: the byte-budget cache API this exercises
/// (`total_bytes`/`budget_bytes`/byte-driven eviction) does not exist on `main`
/// — `main` is a count-capped `LruCache` (capacity 1024) with no byte
/// accounting. So this test pins the NEW behavior introduced by this PR rather
/// than reproducing a failure that compiles on `main`.
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

    // Create 8 keys over 8 DISTINCT binaries. Distinct code is what produces
    // distinct cache entries now that the cache is keyed by code hash (#5268);
    // distinct parameters over one binary deliberately share a single entry, so
    // they would no longer exercise eviction at all.
    let keys = distinct_keys_distinct_code(&mut contract_store, &code, 8);

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
    let params = Parameters::from([].as_ref());
    // We only care that the module compiled and got cached; the validate
    // outcome is irrelevant here, so explicitly discard the must-use result.
    drop(probe_runtime.validate_state(&keys[0], &params, &valid_state, &Default::default()));
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
/// documenting the basis for the default module cache budget. Asserts the
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
    let default_budget = super::super::default_module_cache_budget_bytes();
    println!(
        "MEASURED compiled module size for test_contract_1: {measured} bytes \
         ({:.2} MiB); default module cache budget = {} bytes ({} MiB) \
         holds ~{} such modules",
        measured as f64 / (1024.0 * 1024.0),
        default_budget,
        default_budget / (1024 * 1024),
        if measured > 0 {
            default_budget / measured
        } else {
            0
        },
    );

    assert!(
        (10 * 1024..=16 * 1024 * 1024).contains(&measured),
        "compiled module size {measured} bytes outside expected 10KiB..16MiB range; \
         revisit the module cache budget if the toolchain changed"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

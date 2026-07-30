//! Offline probe: does `get_state_delta(S, summarize_state(S))` return EMPTY?
//!
//! Analysis harness (not a CI test). Driven entirely by environment variables;
//! with none set it is a no-op, so it costs CI nothing.
//!
//! Usage:
//!   PROBE_DUMP=/path/to/dump PROBE_WASM=/path/to/contracts \
//!   PROBE_OUT=/path/to/results.jsonl \
//!   cargo test -p freenet --lib self_delta_probe -- --nocapture --test-threads=1
//!
//! `PROBE_DUMP` must contain `manifest.json` plus `state/<instance>.bin` and
//! `params/<instance>.bin`, as produced by the redb dumper.

use std::sync::Arc;

use freenet_stdlib::prelude::{
    ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, StateSummary,
    WrappedContract, WrappedState,
};

use crate::wasm_runtime::contract::ContractRuntimeInterface;

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', " ")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_delta_probe() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(dump) = std::env::var("PROBE_DUMP") else {
        eprintln!("PROBE_DUMP unset; skipping analysis harness");
        return Ok(());
    };
    let wasmdir = std::env::var("PROBE_WASM").expect("PROBE_WASM");
    let outpath = std::env::var("PROBE_OUT").expect("PROBE_OUT");
    // Optional filter: only instances whose base58 id starts with this prefix.
    let only = std::env::var("PROBE_ONLY").ok();

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(format!("{dump}/manifest.json"))?)?;
    let entries = manifest.as_array().unwrap();

    let temp_dir = crate::util::tests::get_temp_dir();
    let db = crate::contract::storages::Storage::new(temp_dir.path()).await?;
    let contract_store = crate::wasm_runtime::ContractStore::new(
        temp_dir.path().join("contract"),
        1024 * 1024 * 1024,
        db.clone(),
    )?;
    let delegate_store = crate::wasm_runtime::DelegateStore::new(
        temp_dir.path().join("delegate"),
        10_000_000,
        db.clone(),
    )?;
    let secrets_store = crate::wasm_runtime::SecretsStore::new(
        temp_dir.path().join("secrets"),
        Default::default(),
        db,
    )?;
    let mut runtime =
        crate::wasm_runtime::Runtime::build(contract_store, delegate_store, secrets_store, false)?;

    let mut out = std::fs::File::create(&outpath)?;
    use std::io::Write;

    // Cache wasm blobs by code hash so 763 instances of one code read it once.
    let mut code_cache: std::collections::HashMap<String, Arc<ContractCode<'static>>> =
        std::collections::HashMap::new();

    let total = entries.len();
    for (i, e) in entries.iter().enumerate() {
        let inst = e["instance"].as_str().unwrap().to_string();
        if let Some(p) = &only {
            if !inst.starts_with(p.as_str()) {
                continue;
            }
        }
        let Some(code_hash) = e["code_hash"].as_str() else {
            writeln!(
                out,
                "{{\"instance\":\"{inst}\",\"status\":\"no_code_hash\"}}"
            )?;
            continue;
        };
        let code = match code_cache.get(code_hash) {
            Some(c) => c.clone(),
            None => {
                let path = format!("{wasmdir}/{code_hash}.wasm");
                match std::fs::read(&path) {
                    Ok(b) => {
                        let c = Arc::new(ContractCode::from(b));
                        code_cache.insert(code_hash.to_string(), c.clone());
                        c
                    }
                    Err(err) => {
                        writeln!(
                            out,
                            "{{\"instance\":\"{inst}\",\"code_hash\":\"{code_hash}\",\"status\":\"wasm_missing\",\"err\":\"{}\"}}",
                            esc(&err.to_string())
                        )?;
                        continue;
                    }
                }
            }
        };

        let params_bytes = std::fs::read(format!("{dump}/params/{inst}.bin")).unwrap_or_default();
        let state_bytes = std::fs::read(format!("{dump}/state/{inst}.bin"))?;
        let params: Parameters<'static> = Parameters::from(params_bytes.clone());
        let state = WrappedState::new(state_bytes.clone());

        let wc = WrappedContract::new(code.clone(), params.clone());
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(wc));
        let key = container.key();
        let derived = key.encoded_contract_id();
        // Store so the module cache miss path can fetch code by key.
        let _ = runtime.contract_store.store_contract(container.clone());

        let state_len = state_bytes.len();

        // Three summarize calls to test determinism across fresh instances.
        let mut sums: Vec<Result<Vec<u8>, String>> = Vec::new();
        for _ in 0..3 {
            let r = runtime
                .summarize_state(&key, &params, &state)
                .map(|s| s.into_bytes())
                .map_err(|e| e.to_string());
            sums.push(r);
        }

        let (sum_bytes, sum_err) = match &sums[0] {
            Ok(b) => (Some(b.clone()), None),
            Err(e) => (None, Some(e.clone())),
        };
        let deterministic = sums.iter().all(|r| match (r, &sums[0]) {
            (Ok(a), Ok(b)) => a == b,
            (Err(a), Err(b)) => a == b,
            _ => false,
        });

        let (delta_len, delta_err, delta_head) = match &sum_bytes {
            None => (None, Some("summarize failed".to_string()), None),
            Some(sb) => {
                let summary = StateSummary::from(sb.clone());
                match runtime.get_state_delta(&key, &params, &state, &summary) {
                    Ok(d) => {
                        let db = d.into_bytes();
                        let head = db
                            .iter()
                            .take(32)
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>();
                        (Some(db.len()), None, Some(head))
                    }
                    Err(e) => (None, Some(e.to_string()), None),
                }
            }
        };

        writeln!(
            out,
            "{{\"instance\":\"{inst}\",\"derived_id\":\"{derived}\",\"code_hash\":\"{code_hash}\",\"state_len\":{state_len},\"params_len\":{},\"summary_len\":{},\"summary_err\":{},\"deterministic\":{},\"delta_len\":{},\"delta_err\":{},\"delta_head\":{}}}",
            params_bytes.len(),
            sum_bytes
                .as_ref()
                .map(|b| b.len().to_string())
                .unwrap_or("null".into()),
            sum_err
                .map(|e| format!("\"{}\"", esc(&e)))
                .unwrap_or("null".into()),
            deterministic,
            delta_len.map(|l| l.to_string()).unwrap_or("null".into()),
            delta_err
                .map(|e| format!("\"{}\"", esc(&e)))
                .unwrap_or("null".into()),
            delta_head
                .map(|h| format!("\"{h}\""))
                .unwrap_or("null".into()),
        )?;
        out.flush()?;
        if i % 25 == 0 {
            eprintln!("progress {i}/{total}");
        }
    }
    Ok(())
}

//! Offline probe: is a contract's `summarize_state` / `get_state_delta` deterministic,
//! and is its `update_state` idempotent and order-independent (commutative)?
//!
//! Analysis harness, not a CI test. Driven entirely by environment variables; with
//! none set it is a no-op, so it costs CI nothing. Runs the real wasmtime runtime.
//!
//! Usage:
//!   MERGE_WASM=/path/to/raw.wasm MERGE_PARAMS=/path/to/params.bin \
//!   MERGE_STATES=/path/a.bin,/path/b.bin,/path/c.bin \
//!   cargo test -p freenet --lib merge_probe -- --nocapture --test-threads=1

use std::sync::Arc;

use freenet_stdlib::prelude::{
    ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, State, StateSummary,
    UpdateData, WrappedContract, WrappedState,
};

use crate::wasm_runtime::contract::ContractRuntimeInterface;

fn sha(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(b))[..12].to_string()
}

/// Apply a sequence of full-state updates, returning the resulting state bytes.
fn apply_seq(
    rt: &mut crate::wasm_runtime::Runtime,
    key: &freenet_stdlib::prelude::ContractKey,
    params: &Parameters<'static>,
    base: &WrappedState,
    updates: &[Vec<u8>],
) -> Result<Vec<u8>, String> {
    let mut cur = base.clone();
    for u in updates {
        let ud = UpdateData::State(State::from(u.clone()));
        let m = rt
            .update_state(key, params, &cur, &[ud])
            .map_err(|e| e.to_string())?;
        match m.new_state {
            Some(s) => cur = WrappedState::new(s.into_bytes()),
            None => return Err("update returned no new_state".into()),
        }
    }
    Ok(cur.as_ref().to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_probe() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(wasm_path) = std::env::var("MERGE_WASM") else {
        eprintln!("MERGE_WASM unset; skipping analysis harness");
        return Ok(());
    };
    let params_bytes = std::env::var("MERGE_PARAMS")
        .ok()
        .and_then(|p| std::fs::read(p).ok())
        .unwrap_or_default();
    let state_paths: Vec<String> = std::env::var("MERGE_STATES")
        .expect("MERGE_STATES")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

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
    let secrets_store =
        crate::wasm_runtime::SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db)?;
    let mut rt =
        crate::wasm_runtime::Runtime::build(contract_store, delegate_store, secrets_store, false)?;

    let code = Arc::new(ContractCode::from(std::fs::read(&wasm_path)?));
    let params: Parameters<'static> = Parameters::from(params_bytes.clone());
    let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        code.clone(),
        params.clone(),
    )));
    let key = container.key();
    rt.contract_store.store_contract(container)?;
    println!("contract key      : {key}");
    println!("params_len        : {}", params_bytes.len());

    let states: Vec<(String, Vec<u8>)> = state_paths
        .iter()
        .map(|p| {
            let b = std::fs::read(p).unwrap_or_else(|e| panic!("read {p}: {e}"));
            (p.rsplit('/').next().unwrap().to_string(), b)
        })
        .collect();
    for (n, b) in &states {
        println!("state {n:12} len={:<8} sha={}", b.len(), sha(b));
    }

    // ---------- (v) validate_state ----------
    println!("\n===== (v) validate_state =====");
    for (name, sb) in &states {
        let ws = WrappedState::new(sb.clone());
        let related = freenet_stdlib::prelude::RelatedContracts::default();
        match rt.validate_state(&key, &params, &ws, &related) {
            Ok(v) => println!("  {name:26} len={:<9} sha={} => {v:?}", sb.len(), sha(sb)),
            Err(e) => println!(
                "  {name:26} len={:<9} sha={} => RuntimeError: {e}",
                sb.len(),
                sha(sb)
            ),
        }
    }

    // ---------- (a) summarize_state determinism ----------
    println!("\n===== (a) summarize_state determinism (3 calls per state) =====");
    let mut summaries: Vec<Option<Vec<u8>>> = Vec::new();
    for (name, sb) in &states {
        let ws = WrappedState::new(sb.clone());
        let mut runs: Vec<Result<Vec<u8>, String>> = Vec::new();
        for _ in 0..3 {
            runs.push(
                rt.summarize_state(&key, &params, &ws)
                    .map(|s| s.into_bytes())
                    .map_err(|e| e.to_string()),
            );
        }
        let all_same = runs.iter().all(|r| match (r, &runs[0]) {
            (Ok(a), Ok(b)) => a == b,
            (Err(a), Err(b)) => a == b,
            _ => false,
        });
        match &runs[0] {
            Ok(b) => {
                println!(
                    "  {name:12} DETERMINISTIC={all_same:<5} summary_len={:<6} sha={}",
                    b.len(),
                    sha(b)
                );
                for (i, r) in runs.iter().enumerate() {
                    if let Ok(x) = r {
                        println!("      call{i}: len={:<6} sha={}", x.len(), sha(x));
                    }
                }
                summaries.push(Some(b.clone()));
            }
            Err(e) => {
                println!("  {name:12} summarize ERROR: {e}");
                summaries.push(None);
            }
        }
    }

    // ---------- (b) get_state_delta determinism ----------
    println!("\n===== (b) get_state_delta determinism (3 calls, self-summary) =====");
    for (i, (name, sb)) in states.iter().enumerate() {
        let Some(sum) = &summaries[i] else {
            println!("  {name:12} skipped (no summary)");
            continue;
        };
        let ws = WrappedState::new(sb.clone());
        let summary = StateSummary::from(sum.clone());
        let mut runs: Vec<Result<Vec<u8>, String>> = Vec::new();
        for _ in 0..3 {
            runs.push(
                rt.get_state_delta(&key, &params, &ws, &summary)
                    .map(|d| d.into_bytes())
                    .map_err(|e| e.to_string()),
            );
        }
        let all_same = runs.iter().all(|r| match (r, &runs[0]) {
            (Ok(a), Ok(b)) => a == b,
            (Err(a), Err(b)) => a == b,
            _ => false,
        });
        match &runs[0] {
            Ok(d) => println!(
                "  {name:12} DETERMINISTIC={all_same:<5} delta_len={:<8} sha={} (self-delta empty={})",
                d.len(),
                sha(d),
                d.is_empty()
            ),
            Err(e) => println!("  {name:12} delta ERROR (deterministic={all_same}): {e}"),
        }
    }

    // ---------- (c) update_state determinism / idempotence / commutativity ----------
    println!("\n===== (c) update_state =====");
    for (name, sb) in &states {
        let ws = WrappedState::new(sb.clone());
        let r1 = apply_seq(&mut rt, &key, &params, &ws, &[sb.clone()]);
        let r2 = apply_seq(&mut rt, &key, &params, &ws, &[sb.clone()]);
        match (&r1, &r2) {
            (Ok(a), Ok(b)) => println!(
                "  self-merge {name:12} DETERMINISTIC={:<5} IDEMPOTENT(==input)={:<5} out_len={} sha={}",
                a == b,
                a == sb,
                a.len(),
                sha(a)
            ),
            _ => println!("  self-merge {name:12} err r1={r1:?} r2={r2:?}"),
        }
    }

    println!("\n  --- pairwise symmetry: merge(X,Y) vs merge(Y,X) (needs only 2 states) ---");
    for i in 0..states.len() {
        for j in (i + 1)..states.len() {
            let (xn, xb) = &states[i];
            let (yn, yb) = &states[j];
            let xy = apply_seq(
                &mut rt,
                &key,
                &params,
                &WrappedState::new(xb.clone()),
                &[yb.clone()],
            );
            let yx = apply_seq(
                &mut rt,
                &key,
                &params,
                &WrappedState::new(yb.clone()),
                &[xb.clone()],
            );
            match (&xy, &yx) {
                (Ok(a), Ok(b)) => println!(
                    "  X={xn:12} Y={yn:12} SYMMETRIC={:<5} merge(X,Y)=(len {} sha {}) merge(Y,X)=(len {} sha {})  [XY==Y:{} YX==X:{}]",
                    a == b,
                    a.len(),
                    sha(a),
                    b.len(),
                    sha(b),
                    a == yb,
                    b == xb
                ),
                _ => println!(
                    "  X={xn:12} Y={yn:12} err XY={:?} YX={:?}",
                    xy.as_ref().err(),
                    yx.as_ref().err()
                ),
            }
        }
    }

    println!("\n  --- commutativity: base + A + B  vs  base + B + A ---");
    for i in 0..states.len() {
        for j in 0..states.len() {
            for k in 0..states.len() {
                if j >= k || j == i || k == i {
                    continue;
                }
                let (bn, bb) = &states[i];
                let (an, ab) = &states[j];
                let (cn, cb) = &states[k];
                let base = WrappedState::new(bb.clone());
                let ab_order = apply_seq(&mut rt, &key, &params, &base, &[ab.clone(), cb.clone()]);
                let ba_order = apply_seq(&mut rt, &key, &params, &base, &[cb.clone(), ab.clone()]);
                match (&ab_order, &ba_order) {
                    (Ok(x), Ok(y)) => println!(
                        "  base={bn:12} A={an:12} B={cn:12} COMMUTATIVE={:<5} AB(len={} sha={}) BA(len={} sha={})",
                        x == y,
                        x.len(),
                        sha(x),
                        y.len(),
                        sha(y)
                    ),
                    _ => println!(
                        "  base={bn:12} A={an:12} B={cn:12} err AB={:?} BA={:?}",
                        ab_order.as_ref().err(),
                        ba_order.as_ref().err()
                    ),
                }
            }
        }
    }

    Ok(())
}

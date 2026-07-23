//! Probe contract generation. Reuses the `test-contract-integration` WASM
//! (a todo-list contract) with per-run parameters so every run produces
//! fresh contract keys. State layout mirrors the contract's expectations
//! (`tests/test-contract-integration/src/lib.rs`).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct TodoList {
    tasks: Vec<Task>,
    version: u64,
}

#[derive(Serialize, Deserialize)]
struct Task {
    id: u64,
    title: String,
    description: String,
    completed: bool,
    priority: u8,
}

pub struct ProbeContract {
    pub label: String,
    pub contract: ContractContainer,
    pub state: WrappedState,
}

/// Compile the contract crate to WASM and return the bytes.
pub fn compile_contract_wasm(contract_dir: &Path) -> Result<Vec<u8>> {
    const WASM_TARGET: &str = "wasm32-unknown-unknown";

    let status = Command::new("cargo")
        .args(["build", "--lib", "--release", "--target", WASM_TARGET])
        // Nodes reject contracts carrying .debug_* sections; force them off
        // even if a local cargo config enables debuginfo in release builds.
        .env("CARGO_PROFILE_RELEASE_DEBUG", "false")
        .env("CARGO_PROFILE_RELEASE_STRIP", "debuginfo")
        .current_dir(contract_dir)
        .status()
        .with_context(|| format!("running cargo build in {}", contract_dir.display()))?;
    if !status.success() {
        bail!("cargo build failed for {}", contract_dir.display());
    }

    let target_dir = cargo_target_dir(contract_dir)?;
    let name = contract_crate_name(contract_dir)?;
    let wasm_path = target_dir
        .join(WASM_TARGET)
        .join("release")
        .join(name.replace('-', "_"))
        .with_extension("wasm");
    std::fs::read(&wasm_path).with_context(|| format!("reading {}", wasm_path.display()))
}

fn cargo_target_dir(contract_dir: &Path) -> Result<PathBuf> {
    let out = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(contract_dir)
        .output()
        .context("running cargo metadata")?;
    if !out.status.success() {
        bail!("cargo metadata failed in {}", contract_dir.display());
    }
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    let dir = meta["target_directory"]
        .as_str()
        .context("cargo metadata: missing target_directory")?;
    Ok(PathBuf::from(dir))
}

fn contract_crate_name(contract_dir: &Path) -> Result<String> {
    let manifest = std::fs::read_to_string(contract_dir.join("Cargo.toml"))?;
    for line in manifest.lines() {
        if let Some(rest) = line.trim().strip_prefix("name") {
            if let Some(name) = rest.split('"').nth(1) {
                return Ok(name.to_string());
            }
        }
    }
    bail!(
        "could not find package name in {}/Cargo.toml",
        contract_dir.display()
    )
}

/// Build this run's contracts: `small` one-task lists plus one ~1 MB list.
pub fn build_probe_contracts(
    wasm: &[u8],
    run_id: &str,
    small: usize,
) -> Result<Vec<ProbeContract>> {
    let mut out = Vec::with_capacity(small + 1);
    for i in 0..small {
        let label = format!("small-{i}");
        out.push(make_contract(
            wasm,
            run_id,
            &label,
            small_todo_list(run_id, &label)?,
        )?);
    }
    out.push(make_contract(
        wasm,
        run_id,
        "large-1MB",
        large_todo_list(run_id)?,
    )?);
    Ok(out)
}

fn make_contract(wasm: &[u8], run_id: &str, label: &str, state: Vec<u8>) -> Result<ProbeContract> {
    let params = Parameters::from(format!("network-probe/{run_id}/{label}").into_bytes());
    let code = ContractCode::from(wasm.to_vec());
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(code),
        params,
    )));
    Ok(ProbeContract {
        label: label.to_string(),
        contract,
        state: WrappedState::from(state),
    })
}

fn small_todo_list(run_id: &str, label: &str) -> Result<Vec<u8>> {
    let list = TodoList {
        tasks: vec![Task {
            id: 1,
            title: format!("network-probe {run_id} {label}"),
            description: String::new(),
            completed: false,
            priority: 3,
        }],
        version: 1,
    };
    Ok(serde_json::to_vec(&list)?)
}

/// ~1 MB payload: enough ~200-byte tasks to reach the target size.
fn large_todo_list(run_id: &str) -> Result<Vec<u8>> {
    const TARGET_SIZE: usize = 1024 * 1024;
    const APPROX_TASK_SIZE: usize = 200;
    let tasks: Vec<Task> = (0..TARGET_SIZE / APPROX_TASK_SIZE)
        .map(|i| Task {
            id: i as u64,
            title: format!("network-probe {run_id} large payload task {i}"),
            description: format!(
                "Task {i} of the probe's large-payload retention check. \
                 Filler text keeps the serialized size predictable."
            ),
            completed: i % 2 == 0,
            priority: ((i % 5) + 1) as u8,
        })
        .collect();
    Ok(serde_json::to_vec(&TodoList { tasks, version: 1 })?)
}

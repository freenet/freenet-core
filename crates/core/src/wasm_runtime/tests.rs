use std::{path::PathBuf, process::Command};

use crate::util::workspace::get_workspace_target_dir;
use tracing::info;

mod cache;
mod contract;
mod contract_metering;
mod execution_handling;
mod self_delta_probe;
mod time;

pub(crate) fn get_test_module(name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let module_path = {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        path.join("tests").join(name.replace('_', "-"))
    };
    let target = get_workspace_target_dir();
    info!(
        "trying to compile the test contract, target: {}",
        target.display()
    );
    // attempt to compile it
    const RUST_TARGET_ARGS: &[&str] = &["build", "--target"];
    const WASM_TARGET: &str = "wasm32-unknown-unknown";
    let cmd_args = RUST_TARGET_ARGS
        .iter()
        .copied()
        .chain([WASM_TARGET])
        .collect::<Vec<_>>();
    let mut child = Command::new("cargo")
        .args(&cmd_args)
        .current_dir(&module_path)
        .env("CARGO_TARGET_DIR", &target)
        .spawn()?;
    let status = child.wait()?;
    if !status.success() {
        return Err(format!(
            "cargo build failed with {status} for module {name} at {}",
            module_path.display()
        )
        .into());
    }
    let output_file = target
        .join(WASM_TARGET)
        .join("debug")
        .join(name)
        .with_extension("wasm");
    info!("output file: {output_file:?}");
    Ok(std::fs::read(output_file)?)
}

/// Append a WASM custom section carrying `tag`, yielding a module that is
/// byte-distinct (so it compiles to its own `Module` with its own code memory)
/// but semantically identical to `code`.
///
/// Custom sections are `id=0` followed by a LEB128 payload length; the payload
/// is a name (LEB128 length + bytes) plus arbitrary data, and they may appear
/// after any other section.
///
/// Tests that need N genuinely different modules must go through this. Varying
/// the PARAMETERS no longer produces distinct compiled modules — that
/// duplication is exactly what #5268 removed — so a byte-eviction test built on
/// distinct params would pass while measuring nothing.
pub(crate) fn code_variant(code: &[u8], tag: &str) -> Vec<u8> {
    fn leb128(mut value: u32, out: &mut Vec<u8>) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    let name = b"freenet-test-variant";
    let mut payload = Vec::new();
    leb128(name.len() as u32, &mut payload);
    payload.extend_from_slice(name);
    payload.extend_from_slice(tag.as_bytes());

    let mut out = code.to_vec();
    out.push(0x00); // custom section id
    leb128(payload.len() as u32, &mut out);
    out.extend_from_slice(&payload);
    out
}

pub(crate) struct TestSetup {
    #[allow(unused)]
    temp_dir: tempfile::TempDir,
    contract_store: super::ContractStore,
    delegate_store: super::DelegateStore,
    secrets_store: super::SecretsStore,
    contract_key: freenet_stdlib::prelude::ContractKey,
}

pub(crate) async fn setup_test_contract(
    name: &str,
) -> Result<TestSetup, Box<dyn std::error::Error>> {
    use std::sync::Arc;

    use freenet_stdlib::prelude::{
        ContractCode, ContractContainer, ContractWasmAPIVersion, WrappedContract,
    };

    use crate::contract::storages::Storage;
    use crate::util::tests::get_temp_dir;
    let temp_dir = get_temp_dir();

    let db = Storage::new(temp_dir.path()).await?;
    let mut contract_store =
        super::ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())?;
    let delegate_store =
        super::DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())?;
    let secrets_store =
        super::SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db)?;
    let contract_bytes = WrappedContract::new(
        Arc::new(ContractCode::from(get_test_module(name)?)),
        vec![].into(),
    );
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_bytes));
    let contract_key = contract.key();
    contract_store.store_contract(contract)?;
    Ok(TestSetup {
        temp_dir,
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
    })
}

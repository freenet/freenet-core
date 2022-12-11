use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{atomic::AtomicUsize, Arc},
};

use locutus_stdlib::prelude::{
    env_logger, ContractCode, ContractContainer, ContractKey, WasmAPIVersion, WrappedContract,
};

use crate::ContractStore;

mod time;

static TEST_NO: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn test_dir(prefix: &str) -> PathBuf {
    let test_dir = std::env::temp_dir().join("locutus-test").join(format!(
        "{prefix}-test-{}",
        TEST_NO.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    if !test_dir.exists() {
        std::fs::create_dir_all(&test_dir).unwrap();
    }
    test_dir
}

fn get_test_contract(name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let contract_path = {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        path.join("tests").join(name.replace('_', "-"))
    };
    let mut contract_build_path = contract_path
        .join("build/locutus")
        .join(name)
        .with_extension("wasm");
    if !contract_build_path.exists() {
        const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";
        std::env::set_var(TARGET_DIR_VAR, "/home/nachod/.cargo/target/");
        let target = std::env::var(TARGET_DIR_VAR)?;
        println!("trying to compile the test contract, target: {target}");
        // attempt to compile it
        const RUST_TARGET_ARGS: &[&str] = &["build", "--target"];
        const WASI_TARGET: &str = "wasm32-wasi";
        let cmd_args = RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([WASI_TARGET])
            .collect::<Vec<_>>();
        let mut child = Command::new("cargo")
            .args(&cmd_args)
            .current_dir(&contract_path)
            .spawn()?;
        child.wait()?;
        let output_file = Path::new(&target)
            .join("wasm32-wasi")
            .join("debug")
            .join(name)
            .with_extension("wasm");
        println!("output file: {output_file:?}");
        contract_build_path = output_file;
    }
    Ok(std::fs::read(contract_build_path)?)
}

pub fn setup_test_contract(
    name: &str,
) -> Result<(ContractStore, ContractKey), Box<dyn std::error::Error>> {
    let _ = env_logger::try_init();
    let mut store = ContractStore::new(crate::tests::test_dir("contract"), 10_000)?;
    let contract_bytes = WrappedContract::new(
        Arc::new(ContractCode::from(get_test_contract(name)?)),
        vec![].into(),
    );
    let contract = ContractContainer::Wasm(WasmAPIVersion::V1(contract_bytes));
    let key = contract.key();
    store.store_contract(contract)?;
    Ok((store, key))
}

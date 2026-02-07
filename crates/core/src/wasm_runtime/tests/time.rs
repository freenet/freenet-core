//! A test WASM module that checks that the `time` module in the std lib works correctly.

use super::{super::Runtime, TestSetup};
use crate::wasm_runtime::engine::WasmEngine;

#[tokio::test(flavor = "multi_thread")]
async fn now() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract("test_contract_2").await?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    let module = runtime.prepare_contract_call(&contract_key, &vec![].into(), 1_000)?;
    runtime.engine.call_void(&module.handle, "time_func")?;
    std::mem::drop(temp_dir);
    Ok(())
}

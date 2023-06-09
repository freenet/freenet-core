use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use locutus_aft_interface::TokenAllocationRecord;

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let inbox_path = PathBuf::from(MANIFEST);
    let token_allocation_record: TokenAllocationRecord = TokenAllocationRecord::new(HashMap::new());
    let state: Vec<u8> = serde_json::to_vec(&token_allocation_record)?;
    std::fs::write(inbox_path.join("examples").join("initial_state"), state)?;
    Ok(())
}

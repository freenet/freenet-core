// Helper script to create a redb v2 format database for testing migration
// This should be run with redb = "2" to generate the test database file
//
// Usage:
//   1. Temporarily change redb version to "2" in Cargo.toml
//   2. cargo run --bin create_v2_db
//   3. Revert redb version back to "3"
//   4. Run the integration test with the generated database

use redb::{Database, TableDefinition};
use std::path::PathBuf;

const CONTRACT_PARAMS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_params");
const STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data");

    std::fs::create_dir_all(&test_data_dir)?;

    let db_path = test_data_dir.join("redb_v2_test.db");

    // Remove old database if it exists
    let _ = std::fs::remove_file(&db_path);

    println!("Creating v2 database at: {:?}", db_path);

    // Create database with v2 format
    let db = Database::create(&db_path)?;

    // Write some test data
    let txn = db.begin_write()?;
    {
        let mut state_table = txn.open_table(STATE_TABLE)?;
        state_table.insert(b"test_key_1", b"test_value_1")?;
        state_table.insert(b"test_key_2", b"test_value_2")?;

        let mut params_table = txn.open_table(CONTRACT_PARAMS_TABLE)?;
        params_table.insert(b"param_key_1", b"param_value_1")?;
    }
    txn.commit()?;

    println!("Successfully created v2 database with test data");
    println!("Database file: {:?}", db_path);

    // Verify the data was written
    let read_txn = db.begin_read()?;
    let state_table = read_txn.open_table(STATE_TABLE)?;
    let value = state_table.get(b"test_key_1")?;
    println!("Verification: test_key_1 = {:?}", value.map(|v| String::from_utf8_lossy(v.value())));

    Ok(())
}

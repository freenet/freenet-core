// Generator for redb v2 format test database
//
// This standalone binary uses redb 2.x to create a v2 format database
// that can be used to test the migration logic in the main codebase.
//
// Usage:
//   cargo run --manifest-path crates/core/tests/redb_migration_generator/Cargo.toml
//
// Output:
//   Creates: crates/core/tests/data/redb_v2_test.db

use redb::{Database, ReadableTable, TableDefinition};
use std::path::PathBuf;

const CONTRACT_PARAMS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_params");
const STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== redb v2 Database Generator ===");
    println!("redb version: {}", env!("CARGO_PKG_VERSION"));

    // Determine output path
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let test_data_dir = manifest_dir
        .parent()
        .unwrap()
        .join("data");

    std::fs::create_dir_all(&test_data_dir)?;

    let db_path = test_data_dir.join("redb_v2_test.db");

    // Remove old database if it exists
    if db_path.exists() {
        println!("Removing existing database: {:?}", db_path);
        std::fs::remove_file(&db_path)?;
    }

    println!("Creating v2 format database at: {:?}", db_path);

    // Create database with v2 format
    let db = Database::create(&db_path)?;

    // Write test data
    let write_txn = db.begin_write()?;
    {
        let mut state_table = write_txn.open_table(STATE_TABLE)?;
        state_table.insert(b"test_key_1", b"test_value_1")?;
        state_table.insert(b"test_key_2", b"test_value_2")?;
        state_table.insert(b"contract_abc", b"state_data_abc")?;

        let mut params_table = write_txn.open_table(CONTRACT_PARAMS_TABLE)?;
        params_table.insert(b"param_key_1", b"param_value_1")?;
        params_table.insert(b"config_key", b"config_value")?;
    }
    write_txn.commit()?;

    println!("✓ Database created successfully");

    // Verify the data was written
    println!("\nVerifying database contents:");
    let read_txn = db.begin_read()?;
    {
        let state_table = read_txn.open_table(STATE_TABLE)?;
        let mut iter = state_table.iter()?;
        println!("  STATE_TABLE entries:");
        while let Some((key, value)) = iter.next() {
            let (k, v) = (key?, value?);
            println!(
                "    {} = {}",
                String::from_utf8_lossy(k.value()),
                String::from_utf8_lossy(v.value())
            );
        }

        let params_table = read_txn.open_table(CONTRACT_PARAMS_TABLE)?;
        let mut iter = params_table.iter()?;
        println!("  CONTRACT_PARAMS_TABLE entries:");
        while let Some((key, value)) = iter.next() {
            let (k, v) = (key?, value?);
            println!(
                "    {} = {}",
                String::from_utf8_lossy(k.value()),
                String::from_utf8_lossy(v.value())
            );
        }
    }

    println!("\n✓ Verification complete");
    println!("\nOutput file: {:?}", db_path);
    println!("This database can now be used for migration testing.");

    Ok(())
}

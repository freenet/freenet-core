# redb v2 Database Generator

This standalone tool generates a redb v2 format database for testing automatic migration to v3.

## Purpose

The main codebase uses redb 3.x, but we need to test that the automatic migration works when users upgrade from a database created with redb 2.x. This tool solves that problem by being a separate Cargo project that depends on redb 2.x.

## Usage

```bash
# Generate the v2 test database
cargo run --manifest-path crates/core/tests/redb_migration_generator/Cargo.toml

# The database will be created at:
# crates/core/tests/data/redb_v2_test.db
```

## How It Works

1. Uses redb 2.1.3 (last stable v2 release)
2. Creates a database with the same table structure as the main code
3. Populates it with test data
4. Saves it to `../data/redb_v2_test.db`

## Database Schema

Creates two tables matching the production schema:

- **STATE_TABLE** (`"state"`) - Key-value pairs for contract state
- **CONTRACT_PARAMS_TABLE** (`"contract_params"`) - Key-value pairs for contract parameters

## Test Data

The generated database contains:

**STATE_TABLE:**
- `test_key_1` → `test_value_1`
- `test_key_2` → `test_value_2`
- `contract_abc` → `state_data_abc`

**CONTRACT_PARAMS_TABLE:**
- `param_key_1` → `param_value_1`
- `config_key` → `config_value`

## Running the Migration Test

After generating the v2 database:

```bash
# Run the integration test (with redb feature enabled)
cargo test --package freenet --test redb_migration --features redb -- --ignored --nocapture
```

The test verifies that:
1. Opening a v2 database with v3 code triggers automatic migration
2. The old database is backed up with a timestamp
3. A new v3 database is created successfully
4. Normal operation (without migration) continues to work

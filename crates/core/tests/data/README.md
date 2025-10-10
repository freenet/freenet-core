# Test Data Directory

This directory contains test databases for redb migration testing.

## redb_v2_test.db

A database file in redb v2 format, used to test automatic migration to v3.

### Generating the v2 database

To regenerate this test database:

```bash
# Run the generator (uses redb 2.1.3)
cargo run --manifest-path crates/core/tests/redb_migration_generator/Cargo.toml

# The database will be created at:
# crates/core/tests/data/redb_v2_test.db
```

### Database contents

The v2 test database contains:

**STATE_TABLE:**
- `test_key_1` → `test_value_1`
- `test_key_2` → `test_value_2`
- `contract_abc` → `state_data_abc`

**CONTRACT_PARAMS_TABLE:**
- `param_key_1` → `param_value_1`
- `config_key` → `config_value`

### Using in tests

The integration test at `tests/redb_migration.rs` uses this database to verify:
1. Automatic detection of v2 format
2. Backup creation before migration
3. Successful migration to v3 format

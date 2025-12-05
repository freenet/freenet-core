// Integration test for redb database format migration
//
// This test verifies that the automatic migration from redb v2 to v3 format works correctly.
// The test uses a pre-generated v2 format database file stored in tests/data/

use std::path::PathBuf;
use tempfile::TempDir;
use tracing::info;

/// Test that verifies automatic migration from redb v2 to v3 format
///
/// This test:
/// 1. Copies a pre-built v2 format database to a temp directory
/// 2. Attempts to open it with redb v3 (which should trigger migration)
/// 3. Verifies that:
///    - The old database is backed up with a timestamp
///    - A new v3 database is created
///    - The migration completes without errors
#[tokio::test]
#[cfg_attr(not(feature = "redb"), ignore)]
async fn test_automatic_migration_from_v2_to_v3() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "redb")]
    use freenet::storages::redb::ReDb;

    // Path to pre-built v2 database
    let v2_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("redb_v2_test.db");

    assert!(
        v2_db_path.exists(),
        "v2 test database not found at {:?}. Generate it with: cargo run --manifest-path crates/core/tests/redb_migration_generator/Cargo.toml",
        v2_db_path
    );

    // Create temp directory and copy v2 database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");
    std::fs::copy(&v2_db_path, &db_path)?;

    info!("Copied v2 database to test directory: {:?}", db_path);
    info!("This should trigger automatic migration...");

    // Attempt to open with v3 - this should trigger migration
    let result = ReDb::new(temp_dir.path()).await;

    // The migration should succeed
    assert!(
        result.is_ok(),
        "Migration should succeed, but got error: {:?}",
        result.err()
    );

    // Verify that a backup was created
    let backups: Vec<_> = std::fs::read_dir(temp_dir.path())?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.contains("backup")
        })
        .collect();

    assert!(
        !backups.is_empty(),
        "Should have created a backup of the v2 database. Found files: {:?}",
        std::fs::read_dir(temp_dir.path())?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    // Verify the new database exists and can be used
    let _db = result.unwrap();

    // Note: We can't verify the old data was preserved because the migration
    // creates a fresh database. This is expected behavior - the backup is for
    // user reference only.

    info!("✓ Migration completed successfully");
    info!("✓ Backup created: {:?}", backups[0].file_name());

    Ok(())
}

/// Test that verifies the migration logic doesn't interfere with normal operation
#[tokio::test]
#[cfg_attr(not(feature = "redb"), ignore)]
async fn test_normal_operation_without_migration() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "redb")]
    use freenet::storages::redb::ReDb;

    let temp_dir = TempDir::new()?;

    // Create a fresh database - no migration should occur
    let result = ReDb::new(temp_dir.path()).await;
    assert!(
        result.is_ok(),
        "Should create fresh database without issues"
    );

    // Verify no backup files were created
    let backups: Vec<_> = std::fs::read_dir(temp_dir.path())?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.contains("backup")
        })
        .collect();

    assert!(
        backups.is_empty(),
        "Should not create backup when no migration is needed"
    );

    Ok(())
}

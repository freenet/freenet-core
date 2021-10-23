use std::path::Path;
use rusqlite::{Connection};

fn main() {
    // Set sqlite database path as environment variable
    let path_to_db = "/tmp/contracts.db";
    println!("cargo:rustc-env=SQLITE_PATH={}",path_to_db);
    Connection::open(Path::new(path_to_db));
}
use std::env;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use freenet_email_inbox::InboxParams;
use locutus_stdlib::prelude::Parameters;
use rsa::{RsaPrivateKey, pkcs1::DecodeRsaPrivateKey};

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <key-id> <path-to-private-key-file>", args[0]);
        std::process::exit(1);
    }
    println!("args: {:?}", args);
    let key_id = &args[1];
    let key_file_path = &args[2];

    // Open the key file and read its contents into a buffer
    let mut key_file = File::open(key_file_path).unwrap();
    let mut buffer = Vec::new();
    key_file.read_to_end(&mut buffer).unwrap();
    let private_key_str = std::str::from_utf8(&buffer).unwrap();

    let private_key = RsaPrivateKey::from_pkcs1_pem(private_key_str).unwrap();
    let pub_key = private_key.to_public_key();
    let inbox_path = PathBuf::from(MANIFEST);
    let params: Parameters = InboxParams { pub_key }
        .try_into()
        .map_err(|e| format!("{e}"))
        .unwrap();
    let params_file_name = format!("inbox_key_{}", key_id);
    std::fs::write(
        inbox_path.join("examples").join(params_file_name),
        params.into_bytes(),
    )
    .unwrap();
}

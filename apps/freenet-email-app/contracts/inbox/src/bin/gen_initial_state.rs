use std::env;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use freenet_email_inbox::InboxParams;
use locutus_stdlib::prelude::{Parameters, blake2::Digest};
use rsa::{RsaPrivateKey, sha2::Sha256, Pkcs1v15Sign};

const MANIFEST: &str = env!("CARGO_MANIFEST_DIR");
const STATE_UPDATE: &[u8; 8] = &[168, 7, 13, 64, 168, 123, 142, 215];

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

    let private_key =
        <RsaPrivateKey as rsa::pkcs1::DecodeRsaPrivateKey>::from_pkcs1_pem(private_key_str)
            .unwrap();
    let pub_key = private_key.to_public_key();
    let inbox_path = PathBuf::from(MANIFEST);
    let digest = Sha256::digest(STATE_UPDATE).to_vec();
    let signature = private_key
        .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
        .unwrap();

    let state = format!(
        r#"{{
            "messages": [],
            "last_update": "2022-05-10T00:00:00Z",
            "settings": {{
                "minimum_tier": "Day1",
                "private": []
            }},
            "inbox_signature": {}
        }}"#,
        serde_json::to_string(&signature).unwrap()
    );
    std::fs::write(inbox_path.join("examples").join("initial_state.json"), state).unwrap();
}
